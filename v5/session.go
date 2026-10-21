package yamux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	core "github.com/qiangli/yamux"
)

// Session multiplexes independently flow-controlled streams over one ordered
// net.Conn. Open it with Client or Server; the two differ only in the parity of
// the stream identifiers they issue.
type Session struct {
	inner   *core.Session
	config  Config
	newSpan func() (MemoryManager, error)

	mu      sync.Mutex
	streams map[*Stream]struct{}
}

// Client starts a session on the dialling side of conn. mm may be nil; when it
// is not, it is called once per stream to open a memory accounting span.
func Client(conn net.Conn, config *Config, mm func() (MemoryManager, error)) (*Session, error) {
	return newSession(conn, config, mm, core.Client)
}

// Server starts a session on the accepting side of conn. mm may be nil; when it
// is not, it is called once per stream to open a memory accounting span.
func Server(conn net.Conn, config *Config, mm func() (MemoryManager, error)) (*Session, error) {
	return newSession(conn, config, mm, core.Server)
}

func newSession(
	conn net.Conn,
	config *Config,
	mm func() (MemoryManager, error),
	start func(conn io.ReadWriteCloser, config *core.Config) (*core.Session, error),
) (*Session, error) {
	if conn == nil {
		return nil, errors.New("yamux: nil connection")
	}
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	inner, err := start(conn, config.coreConfig())
	if err != nil {
		return nil, err
	}
	return &Session{
		inner:   inner,
		config:  config.clone(),
		newSpan: mm,
		streams: make(map[*Stream]struct{}),
	}, nil
}

// OpenStream opens an outbound stream, giving up if ctx is done first.
func (s *Session) OpenStream(ctx context.Context) (*Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	span, err := s.beginSpan()
	if err != nil {
		return nil, err
	}

	type opened struct {
		stream *core.Stream
		err    error
	}
	ch := make(chan opened, 1)
	go func() {
		stream, err := s.inner.OpenStream()
		ch <- opened{stream, err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			span.done()
			return nil, s.translate(res.err)
		}
		return s.track(res.stream, span), nil
	case <-ctx.Done():
		// The core open is already in flight; reset the stream it hands
		// back so the peer does not keep a stream we abandoned.
		go func() {
			res := <-ch
			if res.stream != nil {
				_ = res.stream.Reset()
			}
			span.done()
		}()
		return nil, ctx.Err()
	}
}

// Open is the net.Conn-typed form of OpenStream.
func (s *Session) Open(ctx context.Context) (net.Conn, error) {
	stream, err := s.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// AcceptStream waits for the peer to open a stream. A stream the memory manager
// refuses is reset and skipped rather than surfaced as an accept failure.
func (s *Session) AcceptStream() (*Stream, error) {
	for {
		inner, err := s.inner.AcceptStream()
		if err != nil {
			return nil, s.translate(err)
		}
		span, err := s.beginSpan()
		if err != nil {
			_ = inner.ResetWithError(goAwayInternalError)
			continue
		}
		return s.track(inner, span), nil
	}
}

// Accept is the net.Conn-typed form of AcceptStream.
func (s *Session) Accept() (net.Conn, error) {
	stream, err := s.AcceptStream()
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// GoAway tells the peer to stop opening streams, reporting the normal code.
// Existing streams stay usable so they can drain.
func (s *Session) GoAway() error {
	return s.translate(s.inner.GoAway())
}

// Close shuts the session and the underlying connection down, reporting the
// normal go-away code to the peer.
func (s *Session) Close() error {
	return s.CloseWithError(goAwayNormal)
}

// CloseWithError is Close with an explicit error code, which the peer reads
// back as the ErrorCode of a remote GoAwayError.
func (s *Session) CloseWithError(errCode uint32) error {
	// Announce first so the code reaches the peer ahead of the transport
	// teardown; a session already shut down reports no error here.
	_ = s.inner.GoAwayWithCode(errCode)
	err := s.inner.Close()

	s.mu.Lock()
	streams := make([]*Stream, 0, len(s.streams))
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	s.streams = make(map[*Stream]struct{})
	s.mu.Unlock()
	for _, stream := range streams {
		stream.span.done()
	}
	return err
}

// Ping sends one ping and reports the round-trip time of its reply.
func (s *Session) Ping() (dur time.Duration, err error) {
	rtt, err := s.inner.Ping()
	return rtt, s.translate(err)
}

// RTT reports the most recent measured round-trip time, or zero before the
// first ping completes.
func (s *Session) RTT() time.Duration { return s.inner.RTT() }

// NumStreams reports how many streams the session still tracks.
func (s *Session) NumStreams() int { return s.inner.NumStreams() }

// IsClosed reports whether the session has reached its terminal state.
func (s *Session) IsClosed() bool { return s.inner.IsClosed() }

// CloseChan is closed once when the session shuts down.
func (s *Session) CloseChan() <-chan struct{} { return s.inner.CloseChan() }

// Addr reports the local address of the underlying transport.
func (s *Session) Addr() net.Addr { return s.inner.Addr() }

// LocalAddr reports the local address of the underlying transport.
func (s *Session) LocalAddr() net.Addr { return s.inner.LocalAddr() }

// RemoteAddr reports the peer address of the underlying transport.
func (s *Session) RemoteAddr() net.Addr { return s.inner.RemoteAddr() }

// beginSpan opens a memory accounting span for one stream and reserves its
// receive window. It returns a nil span when no manager was configured.
func (s *Session) beginSpan() (*memorySpan, error) {
	if s.newSpan == nil {
		return nil, nil
	}
	mm, err := s.newSpan()
	if err != nil {
		return nil, err
	}
	if mm == nil {
		return nil, nil
	}
	reserved := int(s.config.InitialStreamWindowSize)
	if err := mm.ReserveMemory(reserved, streamScopePriority); err != nil {
		mm.Done()
		return nil, err
	}
	return &memorySpan{mm: mm, reserved: reserved}, nil
}

func (s *Session) track(inner *core.Stream, span *memorySpan) *Stream {
	stream := &Stream{inner: inner, session: s, span: span}
	s.mu.Lock()
	s.streams[stream] = struct{}{}
	s.mu.Unlock()
	return stream
}

// untrack drops a finished stream and releases its memory span exactly once.
func (s *Session) untrack(stream *Stream) {
	s.mu.Lock()
	_, tracked := s.streams[stream]
	delete(s.streams, stream)
	s.mu.Unlock()
	if tracked {
		stream.span.done()
	}
}

// translate rewrites a core error onto the v5 surface, folding in the go-away
// codes the session recorded.
func (s *Session) translate(err error) error {
	if err == nil {
		return nil
	}
	if mapped, ok := mapSentinel(err); ok {
		return mapped
	}
	if errors.Is(err, core.ErrRemoteGoAway) {
		code, _ := s.inner.RemoteGoAwayCode()
		return &GoAwayError{Remote: true, ErrorCode: code}
	}
	if errors.Is(err, core.ErrSessionShutdown) {
		code, sent := s.inner.LocalGoAwayCode()
		if !sent {
			return ErrSessionShutdown
		}
		return &GoAwayError{Remote: false, ErrorCode: code}
	}
	// The transport itself failed. If the peer announced a go-away before
	// dropping it, that code is the more useful answer.
	if s.inner.IsClosed() {
		if code, ok := s.inner.RemoteGoAwayCode(); ok {
			return &GoAwayError{Remote: true, ErrorCode: code}
		}
	}
	return err
}

// memorySpan is one stream's reservation against a caller's MemoryManager.
type memorySpan struct {
	mm       MemoryManager
	reserved int
	once     sync.Once
}

// done releases the span's reservation. It is safe on a nil span and on repeat
// calls, so every stream-ending path can call it.
func (m *memorySpan) done() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		m.mm.ReleaseMemory(m.reserved)
		m.mm.Done()
	})
}
