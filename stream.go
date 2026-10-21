package yamux

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// StreamState describes which halves of a Stream remain usable.
type StreamState uint8

const (
	StreamOpen StreamState = iota
	StreamSendClosed
	StreamReceiveClosed
	StreamClosed
)

// Stream is one independently flow-controlled connection within a Session.
type Stream struct {
	session  *Session
	id       uint32
	outbound bool

	mu            sync.Mutex
	buffer        bytes.Buffer
	sendWindow    uint32
	receiveWindow uint32
	acknowledged  bool
	localFIN      bool
	remoteFIN     bool
	readClosed    bool
	resetErr      error
	resetCode     uint32
	resetRemote   bool
	sessionErr    error
	readDeadline  time.Time
	writeDeadline time.Time
	readNotify    chan struct{}
	writeNotify   chan struct{}
}

func newStream(session *Session, id uint32, outbound bool, sendWindow uint32) *Stream {
	return &Stream{
		session:       session,
		id:            id,
		outbound:      outbound,
		sendWindow:    sendWindow,
		receiveWindow: DefaultStreamWindow,
		readNotify:    make(chan struct{}, 1),
		writeNotify:   make(chan struct{}, 1),
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// StreamID returns the wire stream identifier.
func (s *Stream) StreamID() uint32 { return s.id }

// ID returns the wire stream identifier.
func (s *Stream) ID() uint32 { return s.id }

// State returns the stream's current half-close state.
func (s *Stream) State() StreamState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateLocked()
}

func (s *Stream) stateLocked() StreamState {
	if s.resetErr != nil || (s.localFIN && s.remoteFIN) {
		return StreamClosed
	}
	if s.localFIN {
		return StreamSendClosed
	}
	if s.remoteFIN {
		return StreamReceiveClosed
	}
	return StreamOpen
}

// IsClosed reports whether both stream directions are closed or reset.
func (s *Stream) IsClosed() bool { return s.State() == StreamClosed }

// IsWriteClosed reports whether this endpoint has sent FIN or RST.
func (s *Stream) IsWriteClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localFIN || s.resetErr != nil
}

func (s *Stream) markAcknowledged() {
	s.mu.Lock()
	s.acknowledged = true
	s.mu.Unlock()
}

func (s *Stream) receiveData(body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resetErr != nil {
		return nil
	}
	if s.remoteFIN {
		go s.Reset()
		return nil
	}
	if uint32(len(body)) > s.receiveWindow {
		return fmt.Errorf("%w: stream %d", ErrRecvWindowExceeded, s.id)
	}
	s.receiveWindow -= uint32(len(body))
	_, _ = s.buffer.Write(body)
	signal(s.readNotify)
	return nil
}

func (s *Stream) addSendWindow(credit uint32) error {
	s.mu.Lock()
	window, ok := addUint32(s.sendWindow, credit)
	if !ok || window > s.session.config.MaxStreamWindowSize {
		s.mu.Unlock()
		return errors.New("yamux: invalid window update")
	}
	s.sendWindow = window
	s.mu.Unlock()
	signal(s.writeNotify)
	return nil
}

func (s *Stream) markRemoteFIN() bool {
	s.mu.Lock()
	s.remoteFIN = true
	closed := s.localFIN
	s.mu.Unlock()
	signal(s.readNotify)
	return closed
}

func (s *Stream) remoteReset(code uint32) {
	s.mu.Lock()
	s.resetErr = ErrStreamReset
	s.resetCode = code
	s.resetRemote = true
	s.localFIN = true
	s.remoteFIN = true
	s.buffer.Reset()
	s.mu.Unlock()
	signal(s.readNotify)
	signal(s.writeNotify)
}

func (s *Stream) sessionClosed(err error) {
	s.mu.Lock()
	s.sessionErr = err
	s.mu.Unlock()
	signal(s.readNotify)
	signal(s.writeNotify)
}

// Read reads buffered stream bytes and returns credit only after the caller drains them.
func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if s.buffer.Len() != 0 {
			n, _ := s.buffer.Read(p)
			s.receiveWindow += uint32(n)
			deadline := s.readDeadline
			s.mu.Unlock()
			h := Header{Type: TypeWindowUpdate, StreamID: s.id, Length: uint32(n)}
			_ = s.session.queueFrame(h, nil, deadline, false)
			return n, nil
		}
		if s.resetErr != nil {
			err := s.resetErr
			s.mu.Unlock()
			return 0, err
		}
		if s.readClosed {
			s.mu.Unlock()
			return 0, ErrStreamClosed
		}
		if s.remoteFIN {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.sessionErr != nil {
			err := s.sessionErr
			s.mu.Unlock()
			return 0, err
		}
		deadline := s.readDeadline
		s.mu.Unlock()
		if err := waitForSignal(s.readNotify, s.session.done, deadline); err != nil {
			return 0, err
		}
	}
}

// Write splits p into interleaving-sized frames and blocks when credit is exhausted.
func (s *Stream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	written := 0
	for written < len(p) {
		for {
			s.mu.Lock()
			if s.resetErr != nil {
				err := s.resetErr
				s.mu.Unlock()
				return written, err
			}
			if s.localFIN {
				s.mu.Unlock()
				return written, ErrStreamClosed
			}
			if s.sessionErr != nil {
				err := s.sessionErr
				s.mu.Unlock()
				return written, err
			}
			deadline := s.writeDeadline
			if s.sendWindow != 0 {
				n := len(p) - written
				if n > defaultFrameSize {
					n = defaultFrameSize
				}
				if uint32(n) > s.sendWindow {
					n = int(s.sendWindow)
				}
				s.sendWindow -= uint32(n)
				body := append([]byte(nil), p[written:written+n]...)
				s.mu.Unlock()
				h := Header{Type: TypeData, StreamID: s.id, Length: uint32(n)}
				if err := s.session.queueFrame(h, body, deadline, false); err != nil {
					s.mu.Lock()
					s.sendWindow += uint32(n)
					s.mu.Unlock()
					return written, err
				}
				written += n
				break
			}
			s.mu.Unlock()
			if err := waitForSignal(s.writeNotify, s.session.done, deadline); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// CloseWrite sends FIN while leaving the receive half available.
func (s *Stream) CloseWrite() error {
	s.mu.Lock()
	if s.resetErr != nil {
		err := s.resetErr
		s.mu.Unlock()
		return err
	}
	if s.localFIN {
		s.mu.Unlock()
		return nil
	}
	s.localFIN = true
	closed := s.remoteFIN
	deadline := s.writeDeadline
	s.mu.Unlock()

	err := s.session.queueFrame(Header{Type: TypeWindowUpdate, Flags: FlagFIN, StreamID: s.id}, nil, deadline, true)
	signal(s.writeNotify)
	if closed {
		s.session.removeStream(s.id, s)
	}
	return err
}

// CloseRead abandons the receive half. Yamux has no directional read-close frame,
// so this is represented by a reset of the whole stream.
func (s *Stream) CloseRead() error {
	s.mu.Lock()
	s.readClosed = true
	s.buffer.Reset()
	s.mu.Unlock()
	signal(s.readNotify)
	return s.Reset()
}

// Reset immediately tears down both stream directions with RST, reporting the
// normal code (0); see ResetWithError.
func (s *Stream) Reset() error { return s.ResetWithError(GoAwayNormal) }

// ResetWithError tears down both stream directions with RST and carries code in
// the reset frame's length field, where the length is otherwise unused. The
// peer reads it back from ResetError.
func (s *Stream) ResetWithError(code uint32) error {
	s.mu.Lock()
	if s.resetErr != nil {
		s.mu.Unlock()
		return nil
	}
	s.resetErr = ErrStreamReset
	s.resetCode = code
	s.resetRemote = false
	s.localFIN = true
	s.remoteFIN = true
	s.buffer.Reset()
	deadline := s.writeDeadline
	s.mu.Unlock()
	signal(s.readNotify)
	signal(s.writeNotify)
	// Transmit before forgetting the stream. Dropping it from the session
	// first would let a frame that arrives in the meantime take the
	// unknown-stream path and queue a code-less reset ahead of this one,
	// which the peer would then report as the reset code.
	err := s.session.queueFrame(Header{Type: TypeWindowUpdate, Flags: FlagRST, StreamID: s.id, Length: code}, nil, deadline, true)
	s.session.removeStream(s.id, s)
	return err
}

// ResetError reports the error code of a reset stream, whether the peer or this
// endpoint initiated it, and whether the stream was reset at all.
func (s *Stream) ResetError() (code uint32, remote bool, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resetCode, s.resetRemote, s.resetErr != nil
}

// Close gracefully sends FIN, then uses RST if the peer does not finish before
// StreamCloseTimeout.
func (s *Stream) Close() error {
	if err := s.CloseWrite(); err != nil && !errors.Is(err, ErrStreamReset) {
		return err
	}
	deadline := time.Now().Add(s.session.config.StreamCloseTimeout)
	for {
		s.mu.Lock()
		finished := s.remoteFIN || s.resetErr != nil
		sessionErr := s.sessionErr
		s.mu.Unlock()
		if finished {
			s.session.removeStream(s.id, s)
			return nil
		}
		if sessionErr != nil {
			return sessionErr
		}
		if s.session.config.StreamCloseTimeout == 0 {
			return s.Reset()
		}
		if err := waitForSignal(s.readNotify, s.session.done, deadline); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return s.Reset()
			}
			return err
		}
	}
}

// LocalAddr returns the session transport's local address.
func (s *Stream) LocalAddr() net.Addr { return s.session.LocalAddr() }

// RemoteAddr returns the session transport's peer address.
func (s *Stream) RemoteAddr() net.Addr { return s.session.RemoteAddr() }

// SetDeadline sets both stream deadlines.
func (s *Stream) SetDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline = t
	s.writeDeadline = t
	s.mu.Unlock()
	signal(s.readNotify)
	signal(s.writeNotify)
	return nil
}

// SetReadDeadline sets the deadline for future and currently blocked reads.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline = t
	s.mu.Unlock()
	signal(s.readNotify)
	return nil
}

// SetWriteDeadline sets the deadline for future and currently blocked writes.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.writeDeadline = t
	s.mu.Unlock()
	signal(s.writeNotify)
	return nil
}

// Shrink releases an empty stream buffer's retained storage.
func (s *Stream) Shrink() {
	s.mu.Lock()
	if s.buffer.Len() == 0 {
		s.buffer = bytes.Buffer{}
	}
	s.mu.Unlock()
}

func waitForSignal(notify <-chan struct{}, done <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-notify:
			return nil
		case <-done:
			return nil
		}
	}
	d := time.Until(deadline)
	if d <= 0 {
		return deadlineError{}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-notify:
		return nil
	case <-done:
		return nil
	case <-timer.C:
		return deadlineError{}
	}
}

type deadlineError struct{}

func (deadlineError) Error() string   { return "i/o timeout" }
func (deadlineError) Timeout() bool   { return true }
func (deadlineError) Temporary() bool { return true }
func (deadlineError) Unwrap() error   { return os.ErrDeadlineExceeded }
