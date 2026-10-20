package yamux

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

const (
	// DefaultStreamWindow is the protocol-defined initial credit of a stream.
	DefaultStreamWindow = 256 * 1024
	defaultFrameSize    = 16 * 1024
	maxDataFrameSize    = 16 * 1024 * 1024
)

var (
	ErrSessionShutdown        = errors.New("yamux: session is shutting down")
	ErrRemoteGoAway           = errors.New("yamux: remote sent go-away")
	ErrStreamsExhausted       = errors.New("yamux: stream identifiers exhausted")
	ErrDuplicateStream        = errors.New("yamux: duplicate stream")
	ErrRecvWindowExceeded     = errors.New("yamux: receive window exceeded")
	ErrConnectionReset        = errors.New("yamux: connection reset")
	ErrConnectionWriteTimeout = errors.New("yamux: connection write timeout")
	ErrKeepAliveTimeout       = errors.New("yamux: keepalive timeout")
	ErrStreamClosed           = errors.New("yamux: stream closed")
	ErrStreamReset            = errors.New("yamux: stream reset")
)

// Config controls session limits and liveness checks.
type Config struct {
	AcceptBacklog          int
	EnableKeepAlive        bool
	KeepAliveInterval      time.Duration
	KeepAliveTimeout       time.Duration
	ConnectionWriteTimeout time.Duration
	StreamCloseTimeout     time.Duration
	StreamOpenTimeout      time.Duration
	MaxIncomingStreams     int
	MaxStreamWindowSize    uint32
	WriteCoalesceDelay     time.Duration
}

// DefaultConfig returns a fresh configuration with protocol-safe defaults.
func DefaultConfig() *Config {
	return &Config{
		AcceptBacklog:          256,
		EnableKeepAlive:        true,
		KeepAliveInterval:      30 * time.Second,
		KeepAliveTimeout:       30 * time.Second,
		ConnectionWriteTimeout: 10 * time.Second,
		StreamCloseTimeout:     5 * time.Second,
		StreamOpenTimeout:      30 * time.Second,
		MaxIncomingStreams:     512,
		MaxStreamWindowSize:    math.MaxUint32,
	}
}

// VerifyConfig reports invalid values that would make a session unusable.
func VerifyConfig(c *Config) error {
	if c == nil {
		return errors.New("yamux: nil config")
	}
	if c.AcceptBacklog <= 0 {
		return errors.New("yamux: AcceptBacklog must be positive")
	}
	if c.MaxIncomingStreams <= 0 {
		return errors.New("yamux: MaxIncomingStreams must be positive")
	}
	if c.MaxStreamWindowSize < DefaultStreamWindow {
		return fmt.Errorf("yamux: MaxStreamWindowSize must be at least %d", DefaultStreamWindow)
	}
	if c.EnableKeepAlive && (c.KeepAliveInterval <= 0 || c.KeepAliveTimeout <= 0) {
		return errors.New("yamux: keepalive interval and timeout must be positive")
	}
	if c.ConnectionWriteTimeout < 0 || c.StreamCloseTimeout < 0 || c.StreamOpenTimeout < 0 {
		return errors.New("yamux: timeouts must not be negative")
	}
	return nil
}

// SessionState is the lifecycle state of a Session.
type SessionState uint8

const (
	SessionActive SessionState = iota
	SessionClosing
	SessionClosed
)

func (s SessionState) String() string {
	switch s {
	case SessionActive:
		return "Active"
	case SessionClosing:
		return "Closing"
	case SessionClosed:
		return "Closed"
	default:
		return fmt.Sprintf("SessionState(%d)", uint8(s))
	}
}

type sessionMode uint8

const (
	clientMode sessionMode = iota
	serverMode
)

type outboundFrame struct {
	header  Header
	body    []byte
	written chan error
}

type pingResult struct {
	rtt time.Duration
	err error
}

// Session multiplexes streams over one ordered net.Conn.
type Session struct {
	conn   net.Conn
	config Config
	mode   sessionMode

	mu              sync.Mutex
	state           SessionState
	localGoAway     bool
	remoteGoAway    bool
	closeErr        error
	nextID          uint32
	highestRemoteID uint32
	streams         map[uint32]*Stream
	stateNotify     chan struct{}

	acceptCh  chan *Stream
	writeCh   chan outboundFrame
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	pingSerial  sync.Mutex
	pingMu      sync.Mutex
	nextPingID  uint32
	pingPending bool
	pingID      uint32
	pingSent    time.Time
	pingCh      chan pingResult
	lastRTT     time.Duration
}

// Client starts a client-mode session, whose outbound stream identifiers are odd.
func Client(conn net.Conn, config *Config) (*Session, error) {
	return newSession(conn, config, clientMode)
}

// Server starts a server-mode session, whose outbound stream identifiers are even.
func Server(conn net.Conn, config *Config) (*Session, error) {
	return newSession(conn, config, serverMode)
}

func newSession(conn net.Conn, config *Config, mode sessionMode) (*Session, error) {
	if conn == nil {
		return nil, errors.New("yamux: nil connection")
	}
	if config == nil {
		config = DefaultConfig()
	}
	cfg := *config
	if err := VerifyConfig(&cfg); err != nil {
		return nil, err
	}
	s := &Session{
		conn:        conn,
		config:      cfg,
		mode:        mode,
		state:       SessionActive,
		streams:     make(map[uint32]*Stream),
		stateNotify: make(chan struct{}, 1),
		acceptCh:    make(chan *Stream, cfg.AcceptBacklog),
		writeCh:     make(chan outboundFrame, 256),
		done:        make(chan struct{}),
	}
	if mode == clientMode {
		s.nextID = 1
	} else {
		s.nextID = 2
	}
	s.wg.Add(2)
	go s.readLoop()
	go s.writeLoop()
	if cfg.EnableKeepAlive {
		s.wg.Add(1)
		go s.keepaliveLoop()
	}
	return s, nil
}

// State returns the current session lifecycle state.
func (s *Session) State() SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// IsClosed reports whether the session reached its terminal state.
func (s *Session) IsClosed() bool { return s.State() == SessionClosed }

// CloseChan is closed exactly once when the session reaches Closed.
func (s *Session) CloseChan() <-chan struct{} { return s.done }

func (s *Session) signalState() {
	select {
	case s.stateNotify <- struct{}{}:
	default:
	}
}

// OpenStream creates an outbound stream and transmits its SYN.
func (s *Session) OpenStream() (*Stream, error) {
	s.mu.Lock()
	if s.state == SessionClosed {
		err := s.closeErr
		if err == nil {
			err = ErrSessionShutdown
		}
		s.mu.Unlock()
		return nil, err
	}
	if s.remoteGoAway {
		s.mu.Unlock()
		return nil, ErrRemoteGoAway
	}
	if s.localGoAway || s.state != SessionActive {
		s.mu.Unlock()
		return nil, ErrSessionShutdown
	}
	if len(s.streams) >= s.config.MaxIncomingStreams*2 {
		s.mu.Unlock()
		return nil, errors.New("yamux: maximum number of streams reached")
	}
	id := s.nextID
	if id == 0 {
		s.mu.Unlock()
		return nil, ErrStreamsExhausted
	}
	if id > math.MaxUint32-2 {
		s.nextID = 0
	} else {
		s.nextID += 2
	}
	stream := newStream(s, id, true, DefaultStreamWindow)
	s.streams[id] = stream
	s.mu.Unlock()

	h := Header{Type: TypeWindowUpdate, Flags: FlagSYN, StreamID: id}
	deadline := time.Time{}
	if s.config.StreamOpenTimeout > 0 {
		deadline = time.Now().Add(s.config.StreamOpenTimeout)
	}
	if err := s.queueFrame(h, nil, deadline, true); err != nil {
		s.removeStream(id, stream)
		stream.sessionClosed(err)
		return nil, err
	}
	return stream, nil
}

// Open is the net.Conn-typed compatibility form of OpenStream.
func (s *Session) Open() (net.Conn, error) {
	return s.OpenStream()
}

// AcceptStream waits for the peer to open a stream.
func (s *Session) AcceptStream() (*Stream, error) {
	for {
		select {
		case stream := <-s.acceptCh:
			return stream, nil
		default:
		}

		s.mu.Lock()
		state := s.state
		remoteGoAway := s.remoteGoAway
		localGoAway := s.localGoAway
		err := s.closeErr
		s.mu.Unlock()
		if state == SessionClosed {
			if err == nil {
				err = ErrSessionShutdown
			}
			return nil, err
		}
		if remoteGoAway {
			return nil, ErrRemoteGoAway
		}
		if localGoAway {
			return nil, ErrSessionShutdown
		}

		select {
		case stream := <-s.acceptCh:
			return stream, nil
		case <-s.stateNotify:
		case <-s.done:
		}
	}
}

// Accept is the net.Conn-typed compatibility form of AcceptStream.
func (s *Session) Accept() (net.Conn, error) { return s.AcceptStream() }

// NumStreams returns the number of streams still tracked by the session.
func (s *Session) NumStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

// GoAway stops new streams while leaving existing streams available to drain.
func (s *Session) GoAway() error {
	s.mu.Lock()
	if s.state == SessionClosed {
		err := s.closeErr
		if err == nil {
			err = ErrSessionShutdown
		}
		s.mu.Unlock()
		return err
	}
	if s.localGoAway {
		s.mu.Unlock()
		return nil
	}
	s.localGoAway = true
	s.state = SessionClosing
	s.mu.Unlock()
	s.signalState()
	return s.queueFrame(Header{Type: TypeGoAway}, nil, time.Time{}, true)
}

// Close sends a normal GoAway and terminates the transport and all goroutines.
func (s *Session) Close() error {
	s.mu.Lock()
	alreadyClosed := s.state == SessionClosed
	s.mu.Unlock()
	if !alreadyClosed {
		_ = s.GoAway()
		s.failSession(ErrSessionShutdown)
	}
	s.wg.Wait()
	return nil
}

// LocalAddr returns the address of the underlying transport.
func (s *Session) LocalAddr() net.Addr { return s.conn.LocalAddr() }

// RemoteAddr returns the address of the underlying transport.
func (s *Session) RemoteAddr() net.Addr { return s.conn.RemoteAddr() }

// RTT returns the most recent matched ping round-trip time, or zero before one completes.
func (s *Session) RTT() time.Duration {
	s.pingMu.Lock()
	defer s.pingMu.Unlock()
	return s.lastRTT
}

// Ping sends one ping and waits for the pong carrying exactly the same identifier.
func (s *Session) Ping() (time.Duration, error) {
	s.pingSerial.Lock()
	defer s.pingSerial.Unlock()

	s.mu.Lock()
	closed := s.state == SessionClosed
	err := s.closeErr
	s.mu.Unlock()
	if closed {
		if err == nil {
			err = ErrSessionShutdown
		}
		return 0, err
	}

	s.pingMu.Lock()
	id := s.nextPingID
	s.nextPingID++
	ch := make(chan pingResult, 1)
	s.pingPending = true
	s.pingID = id
	s.pingSent = time.Now()
	s.pingCh = ch
	s.pingMu.Unlock()

	if err := s.queueFrame(Header{Type: TypePing, Flags: FlagSYN, Length: id}, nil, time.Time{}, true); err != nil {
		s.cancelPing(ch)
		return 0, err
	}
	timer := time.NewTimer(s.config.KeepAliveTimeout)
	defer timer.Stop()
	select {
	case result := <-ch:
		return result.rtt, result.err
	case <-timer.C:
		s.cancelPing(ch)
		return 0, ErrKeepAliveTimeout
	case <-s.done:
		s.cancelPing(ch)
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		if err == nil {
			err = ErrSessionShutdown
		}
		return 0, err
	}
}

func (s *Session) cancelPing(ch chan pingResult) {
	s.pingMu.Lock()
	if s.pingPending && s.pingCh == ch {
		s.pingPending = false
		s.pingCh = nil
	}
	s.pingMu.Unlock()
}

func (s *Session) keepaliveLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.KeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if _, err := s.Ping(); err != nil {
				if errors.Is(err, ErrKeepAliveTimeout) {
					s.failSession(err)
				}
				return
			}
		case <-s.done:
			return
		}
	}
}

func (s *Session) queueFrame(h Header, body []byte, deadline time.Time, wait bool) error {
	frame := outboundFrame{header: h, body: body}
	if wait {
		frame.written = make(chan error, 1)
	}
	if err := sendBefore(s.writeCh, frame, deadline, s.done); err != nil {
		return err
	}
	if !wait {
		return nil
	}
	select {
	case err := <-frame.written:
		return err
	case <-s.done:
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		if err == nil {
			err = ErrSessionShutdown
		}
		return err
	}
}

func sendBefore(ch chan<- outboundFrame, frame outboundFrame, deadline time.Time, done <-chan struct{}) error {
	if deadline.IsZero() {
		select {
		case ch <- frame:
			return nil
		case <-done:
			return ErrSessionShutdown
		}
	}
	d := time.Until(deadline)
	if d <= 0 {
		return deadlineError{}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case ch <- frame:
		return nil
	case <-timer.C:
		return deadlineError{}
	case <-done:
		return ErrSessionShutdown
	}
}

func (s *Session) writeLoop() {
	defer s.wg.Done()
	for {
		select {
		case frame := <-s.writeCh:
			err := s.writeFrame(frame)
			if frame.written != nil {
				frame.written <- err
			}
			if err != nil {
				s.failSession(err)
				return
			}
		case <-s.done:
			return
		}
	}
}

func (s *Session) writeFrame(frame outboundFrame) error {
	if timeout := s.config.ConnectionWriteTimeout; timeout > 0 {
		if err := s.conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer s.conn.SetWriteDeadline(time.Time{})
	}
	b := frame.header.Encode()
	if err := writeAll(s.conn, b[:]); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return fmt.Errorf("%w: %v", ErrConnectionWriteTimeout, err)
		}
		return err
	}
	if len(frame.body) != 0 {
		if err := writeAll(s.conn, frame.body); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return fmt.Errorf("%w: %v", ErrConnectionWriteTimeout, err)
			}
			return err
		}
	}
	return nil
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) != 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func (s *Session) readLoop() {
	defer s.wg.Done()
	var raw [HeaderSize]byte
	for {
		if _, err := io.ReadFull(s.conn, raw[:]); err != nil {
			select {
			case <-s.done:
				return
			default:
				s.failSession(fmt.Errorf("yamux: read frame: %w", err))
				return
			}
		}
		h, err := DecodeHeader(raw[:])
		if err == nil {
			err = s.validateHeader(h)
		}
		if err != nil {
			s.failSession(err)
			return
		}
		var body []byte
		if h.Type == TypeData {
			if h.Length > maxDataFrameSize {
				s.failSession(fmt.Errorf("yamux: data frame too large: %d", h.Length))
				return
			}
			body = make([]byte, int(h.Length))
			if _, err := io.ReadFull(s.conn, body); err != nil {
				s.failSession(fmt.Errorf("yamux: read data: %w", err))
				return
			}
		}
		if err := s.handleFrame(h, body); err != nil {
			s.failSession(err)
			return
		}
	}
}

func (s *Session) validateHeader(h Header) error {
	if err := h.ValidateFlags(); err != nil {
		return err
	}
	if h.Flags.Contains(FlagFIN) && h.Flags.Contains(FlagRST) {
		return fmt.Errorf("%w: FIN and RST", ErrUnexpectedFlag)
	}
	switch h.Type {
	case TypeData, TypeWindowUpdate:
		if h.StreamID == 0 {
			return errors.New("yamux: stream frame used session identifier")
		}
	case TypePing:
		if h.StreamID != 0 || (h.Flags != FlagSYN && h.Flags != FlagACK) {
			return fmt.Errorf("%w: invalid ping", ErrUnexpectedFlag)
		}
	case TypeGoAway:
		if h.StreamID != 0 || h.Flags != 0 {
			return fmt.Errorf("%w: invalid go-away", ErrUnexpectedFlag)
		}
	}
	return nil
}

func (s *Session) handleFrame(h Header, body []byte) error {
	switch h.Type {
	case TypePing:
		return s.handlePing(h)
	case TypeGoAway:
		s.mu.Lock()
		if s.state != SessionClosed {
			s.remoteGoAway = true
			s.state = SessionClosing
		}
		s.mu.Unlock()
		s.signalState()
		return nil
	case TypeData, TypeWindowUpdate:
		return s.handleStreamFrame(h, body)
	default:
		return ErrInvalidMsgType
	}
}

func (s *Session) handlePing(h Header) error {
	if h.Flags == FlagSYN {
		return s.queueFrame(Header{Type: TypePing, Flags: FlagACK, Length: h.Length}, nil, time.Time{}, false)
	}
	s.pingMu.Lock()
	if !s.pingPending || h.Length != s.pingID {
		s.pingMu.Unlock()
		return fmt.Errorf("yamux: unexpected pong %d", h.Length)
	}
	rtt := time.Since(s.pingSent)
	ch := s.pingCh
	s.pingPending = false
	s.pingCh = nil
	s.lastRTT = rtt
	s.pingMu.Unlock()
	ch <- pingResult{rtt: rtt}
	return nil
}

func (s *Session) handleStreamFrame(h Header, body []byte) error {
	s.mu.Lock()
	stream := s.streams[h.StreamID]
	s.mu.Unlock()
	if h.Flags.Contains(FlagSYN) {
		if stream != nil {
			return fmt.Errorf("%w: %d", ErrDuplicateStream, h.StreamID)
		}
		return s.handleNewStream(h, body)
	}
	if stream == nil {
		return s.queueFrame(Header{Type: TypeWindowUpdate, Flags: FlagRST, StreamID: h.StreamID}, nil, time.Time{}, false)
	}
	if h.Flags.Contains(FlagRST) {
		stream.remoteReset()
		s.removeStream(h.StreamID, stream)
		return nil
	}
	if h.Flags.Contains(FlagACK) {
		stream.markAcknowledged()
	}
	if h.Type == TypeData && len(body) != 0 {
		if err := stream.receiveData(body); err != nil {
			return err
		}
	}
	if h.Type == TypeWindowUpdate && h.Length != 0 {
		if err := stream.addSendWindow(h.Length); err != nil {
			return err
		}
	}
	if h.Flags.Contains(FlagFIN) {
		if stream.markRemoteFIN() {
			s.removeStream(h.StreamID, stream)
		}
	}
	return nil
}

func (s *Session) handleNewStream(h Header, body []byte) error {
	if h.Flags.Contains(FlagRST) {
		return fmt.Errorf("%w: SYN with RST", ErrUnexpectedFlag)
	}
	if !s.validRemoteID(h.StreamID) {
		return fmt.Errorf("yamux: invalid remote stream id %d", h.StreamID)
	}

	s.mu.Lock()
	if h.StreamID <= s.highestRemoteID {
		s.mu.Unlock()
		return fmt.Errorf("yamux: non-monotonic remote stream id %d", h.StreamID)
	}
	if s.localGoAway || s.state == SessionClosed {
		s.mu.Unlock()
		return s.queueFrame(Header{Type: TypeWindowUpdate, Flags: FlagRST, StreamID: h.StreamID}, nil, time.Time{}, false)
	}
	incoming := 0
	for _, existing := range s.streams {
		if !existing.outbound {
			incoming++
		}
	}
	if incoming >= s.config.MaxIncomingStreams {
		s.mu.Unlock()
		return s.queueFrame(Header{Type: TypeWindowUpdate, Flags: FlagRST, StreamID: h.StreamID}, nil, time.Time{}, false)
	}
	sendWindow := uint32(DefaultStreamWindow)
	if h.Type == TypeWindowUpdate {
		var ok bool
		sendWindow, ok = addUint32(sendWindow, h.Length)
		if !ok || sendWindow > s.config.MaxStreamWindowSize {
			s.mu.Unlock()
			return errors.New("yamux: invalid initial window update")
		}
	}
	stream := newStream(s, h.StreamID, false, sendWindow)
	if h.Type == TypeData && len(body) != 0 {
		if err := stream.receiveData(body); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	if h.Flags.Contains(FlagFIN) {
		stream.markRemoteFIN()
	}
	s.highestRemoteID = h.StreamID
	s.streams[h.StreamID] = stream
	s.mu.Unlock()

	select {
	case s.acceptCh <- stream:
		if err := s.queueFrame(Header{Type: TypeWindowUpdate, Flags: FlagACK, StreamID: h.StreamID}, nil, time.Time{}, false); err != nil {
			return err
		}
		stream.markAcknowledged()
		return nil
	default:
		s.removeStream(h.StreamID, stream)
		stream.remoteReset()
		return s.queueFrame(Header{Type: TypeWindowUpdate, Flags: FlagRST, StreamID: h.StreamID}, nil, time.Time{}, false)
	}
}

func (s *Session) validRemoteID(id uint32) bool {
	if id == 0 {
		return false
	}
	if s.mode == clientMode {
		return id%2 == 0
	}
	return id%2 == 1
}

func addUint32(a, b uint32) (uint32, bool) {
	c := a + b
	return c, c >= a
}

func (s *Session) removeStream(id uint32, stream *Stream) {
	s.mu.Lock()
	if s.streams[id] == stream {
		delete(s.streams, id)
	}
	s.mu.Unlock()
	s.signalState()
}

func (s *Session) failSession(err error) {
	if err == nil {
		err = ErrSessionShutdown
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.state = SessionClosed
		s.closeErr = err
		streams := make([]*Stream, 0, len(s.streams))
		for _, stream := range s.streams {
			streams = append(streams, stream)
		}
		s.streams = make(map[uint32]*Stream)
		s.mu.Unlock()
		for _, stream := range streams {
			stream.sessionClosed(err)
		}
		close(s.done)
		_ = s.conn.Close()
		s.signalState()

		s.pingMu.Lock()
		if s.pingPending {
			ch := s.pingCh
			s.pingPending = false
			s.pingCh = nil
			ch <- pingResult{err: err}
		}
		s.pingMu.Unlock()
	})
}
