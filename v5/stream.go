package yamux

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	core "github.com/qiangli/yamux"
)

// Stream is one flow-controlled connection inside a Session and implements
// net.Conn. Both halves can be closed independently: CloseWrite sends FIN to
// the peer, while CloseRead is local only, because the protocol has no
// read-close frame.
type Stream struct {
	inner   *core.Stream
	session *Session
	span    *memorySpan

	// readClosed records a local CloseRead or Close. Yamux cannot tell the
	// peer about it, so reads are refused here instead.
	readClosed atomic.Bool
}

// StreamID reports the stream's wire identifier.
func (s *Stream) StreamID() uint32 { return s.inner.StreamID() }

// Session reports the session the stream belongs to.
func (s *Stream) Session() *Session { return s.session }

// Read reads from the stream. It returns io.EOF once the peer has closed its
// write half, and a *StreamError once the stream has been reset.
func (s *Stream) Read(b []byte) (n int, err error) {
	if s.readClosed.Load() {
		return 0, s.resetError()
	}
	n, err = s.inner.Read(b)
	return n, s.translate(err)
}

// Write writes to the stream, blocking while the peer's receive window is full.
func (s *Stream) Write(b []byte) (int, error) {
	n, err := s.inner.Write(b)
	return n, s.translate(err)
}

// CloseWrite sends FIN, so the peer sees io.EOF, and leaves the receive half
// usable.
func (s *Stream) CloseWrite() error {
	return s.translate(s.inner.CloseWrite())
}

// CloseRead stops this endpoint from reading. The protocol has no frame for a
// one-sided read close, so it takes effect locally: later reads report the
// stream as reset while the write half keeps working, and the peer stalls once
// it has filled the receive window.
func (s *Stream) CloseRead() error {
	s.readClosed.Store(true)
	return nil
}

// Close closes both halves: FIN goes to the peer and local reads stop. It does
// not wait for the peer to close its own write half.
func (s *Stream) Close() error {
	s.readClosed.Store(true)
	err := s.inner.CloseWrite()
	s.session.untrack(s)
	return s.translate(err)
}

// Reset tears the stream down in both directions with the normal code.
func (s *Stream) Reset() error { return s.ResetWithError(goAwayNormal) }

// ResetWithError tears the stream down in both directions and reports errCode
// to the peer, which reads it back as the ErrorCode of a remote StreamError.
func (s *Stream) ResetWithError(errCode uint32) error {
	s.readClosed.Store(true)
	err := s.inner.ResetWithError(errCode)
	s.session.untrack(s)
	return s.translate(err)
}

// LocalAddr reports the session transport's local address.
func (s *Stream) LocalAddr() net.Addr { return s.inner.LocalAddr() }

// RemoteAddr reports the session transport's peer address.
func (s *Stream) RemoteAddr() net.Addr { return s.inner.RemoteAddr() }

// SetDeadline sets both stream deadlines.
func (s *Stream) SetDeadline(t time.Time) error { return s.inner.SetDeadline(t) }

// SetReadDeadline sets the deadline for reads, including one already blocked.
func (s *Stream) SetReadDeadline(t time.Time) error { return s.inner.SetReadDeadline(t) }

// SetWriteDeadline sets the deadline for writes, including one already blocked.
func (s *Stream) SetWriteDeadline(t time.Time) error { return s.inner.SetWriteDeadline(t) }

// resetError is the error a locally read-closed stream reports. It carries the
// reset code when the stream really was reset, and the plain reset sentinel
// after a Close or CloseRead, which put no reset on the wire.
func (s *Stream) resetError() error {
	if code, remote, ok := s.inner.ResetError(); ok {
		return &StreamError{ErrorCode: code, Remote: remote}
	}
	return ErrStreamReset
}

// translate rewrites a core stream error onto the v5 surface.
func (s *Stream) translate(err error) error {
	if err == nil {
		return nil
	}
	// The core reports a peer FIN as a bare io.EOF; a dead transport wraps
	// its own io.EOF, so compare by identity and leave EOF alone.
	if err == io.EOF {
		return io.EOF
	}
	if code, remote, ok := s.inner.ResetError(); ok && errorsIsReset(err) {
		return &StreamError{ErrorCode: code, Remote: remote}
	}
	return s.session.translate(err)
}

func errorsIsReset(err error) bool {
	return errors.Is(err, core.ErrStreamReset)
}
