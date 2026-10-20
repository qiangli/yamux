package yamux

import (
	"context"
	"io"
	"net"
	"time"
)

// NetError is the timeout/temporary error type exposed as ErrTimeout.
type NetError struct {
	msg       string
	timeout   bool
	temporary bool
}

func (e *NetError) Error() string   { return e.msg }
func (e *NetError) Timeout() bool   { return e.timeout }
func (e *NetError) Temporary() bool { return e.temporary }

// ErrTimeout is the deadline-exceeded sentinel used by streams and sessions.
var ErrTimeout error = &NetError{msg: "i/o deadline reached", timeout: true, temporary: true}

// Clone returns a shallow copy of the config.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	clone := *c
	return &clone
}

// Addr returns the local address of the underlying transport.
func (s *Session) Addr() net.Addr {
	return s.LocalAddr()
}

// AcceptStreamWithContext waits for the peer to open a stream, respecting ctx.
func (s *Session) AcceptStreamWithContext(ctx context.Context) (*Stream, error) {
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
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Session returns the parent session of the stream.
func (s *Stream) Session() *Session {
	return s.session
}

// toNetConn wraps an io.ReadWriteCloser as net.Conn when needed.
func toNetConn(rwc io.ReadWriteCloser) net.Conn {
	if nc, ok := rwc.(net.Conn); ok {
		return nc
	}
	return &rwcConn{ReadWriteCloser: rwc}
}

type dummyAddr struct{}

func (dummyAddr) Network() string { return "yamux" }
func (dummyAddr) String() string  { return "yamux" }

type rwcConn struct {
	io.ReadWriteCloser
}

func (c *rwcConn) LocalAddr() net.Addr                { return dummyAddr{} }
func (c *rwcConn) RemoteAddr() net.Addr               { return dummyAddr{} }
func (c *rwcConn) SetDeadline(t time.Time) error      { return nil }
func (c *rwcConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *rwcConn) SetWriteDeadline(t time.Time) error { return nil }
