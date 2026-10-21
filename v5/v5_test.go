package yamux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func testConfig() *Config {
	c := DefaultConfig()
	c.LogOutput = io.Discard
	c.EnableKeepAlive = false
	return c
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func pair(t *testing.T, mm func() (MemoryManager, error)) (*Session, *Session) {
	t.Helper()
	cConn, sConn := net.Pipe()
	client, err := Client(cConn, testConfig(), mm)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	server, err := Server(sConn, testConfig(), mm)
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

// TestAPIShape pins the exported surface a go-libp2p muxer converts to and
// from. go-libp2p declares `type conn yamux.Session`, `type stream
// yamux.Stream` and `type Transport yamux.Config`, so these conversions must
// keep compiling for the replace directive to work.
func TestAPIShape(t *testing.T) {
	var (
		_ net.Conn  = (*Stream)(nil)
		_ net.Error = (*Error)(nil)
		_ net.Error = (*GoAwayError)(nil)
		_ error     = (*StreamError)(nil)
	)

	type conn Session
	type stream Stream
	type transport Config

	cfg := DefaultConfig()
	tr := (*transport)(cfg)
	if (*Config)(tr) != cfg {
		t.Fatal("Config must round-trip through a defined type")
	}
	cfg.MaxStreamWindowSize = 16 * 1024 * 1024
	cfg.LogOutput = io.Discard
	cfg.ReadBufSize = 0
	cfg.MaxIncomingStreams = ^uint32(0)
	if err := VerifyConfig(cfg); err != nil {
		t.Fatalf("VerifyConfig must accept a go-libp2p-shaped config: %v", err)
	}

	client, _ := pair(t, nil)
	if _ = (*conn)(client); client.Addr() == nil {
		t.Fatal("Addr must be set")
	}
	cStream, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = (*stream)(cStream)
	if cStream.Session() != client {
		t.Fatal("Stream.Session must report the parent session")
	}
	if cStream.StreamID() == 0 {
		t.Fatal("StreamID must be non-zero")
	}
	if client.NumStreams() != 1 {
		t.Fatalf("NumStreams = %d, want 1", client.NumStreams())
	}
}

// TestOpenAcceptBothDirections round-trips a payload larger than one window in
// both directions, through the net.Conn forms as well.
func TestOpenAcceptBothDirections(t *testing.T) {
	client, server := pair(t, nil)
	payload := bytes.Repeat([]byte("yamux-v5"), 50*1024) // 400 KiB

	for _, dir := range []struct {
		name             string
		opener, accepter *Session
	}{
		{"client-to-server", client, server},
		{"server-to-client", server, client},
	} {
		t.Run(dir.name, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				in, err := dir.accepter.Accept()
				if err != nil {
					t.Errorf("Accept: %v", err)
					return
				}
				defer in.Close()
				if _, err := io.Copy(in, io.LimitReader(in, int64(len(payload)))); err != nil {
					t.Errorf("echo: %v", err)
				}
			}()

			out, err := dir.opener.Open(testCtx(t))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer out.Close()
			go func() {
				if _, err := out.Write(payload); err != nil {
					t.Errorf("Write: %v", err)
				}
			}()
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(out, got); err != nil {
				t.Fatalf("ReadFull: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("payload mismatch")
			}
			wg.Wait()
		})
	}
}

// TestResetWithErrorCode checks that a reset code survives the wire in both
// directions and that errors.Is still matches ErrStreamReset.
func TestResetWithErrorCode(t *testing.T) {
	client, server := pair(t, nil)

	got := make(chan error, 1)
	go func() {
		in, err := server.AcceptStream()
		if err != nil {
			got <- err
			return
		}
		got <- readUntilError(in)
	}()

	out, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := out.Write([]byte("hi")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := out.ResetWithError(77); err != nil {
		t.Fatalf("ResetWithError: %v", err)
	}

	select {
	case err := <-got:
		var se *StreamError
		if !errors.As(err, &se) {
			t.Fatalf("peer error %v (%T), want *StreamError", err, err)
		}
		if se.ErrorCode != 77 || !se.Remote {
			t.Fatalf("StreamError{%d, remote=%v}, want {77, remote=true}", se.ErrorCode, se.Remote)
		}
		if !errors.Is(err, ErrStreamReset) {
			t.Fatal("a StreamError must match ErrStreamReset")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the reset")
	}

	// the resetting side reports the same code, as a local reset
	err = readUntilError(out)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("local error %v (%T), want *StreamError", err, err)
	}
	if se.ErrorCode != 77 || se.Remote {
		t.Fatalf("local StreamError{%d, remote=%v}, want {77, remote=false}", se.ErrorCode, se.Remote)
	}
}

// readUntilError drains a stream until a read fails, so a reset that races
// data already in flight is still observed. It gives up rather than spinning
// forever if the stream stays healthy.
func readUntilError(c net.Conn) error {
	buf := make([]byte, 64)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.Read(buf); err != nil {
			return err
		}
	}
	return nil
}

// TestCloseWithErrorCode checks the session error code reaches the peer as a
// remote GoAwayError that matches ErrRemoteGoAway.
func TestCloseWithErrorCode(t *testing.T) {
	cConn, sConn := net.Pipe()
	client, err := Client(cConn, testConfig(), nil)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	server, err := Server(sConn, testConfig(), nil)
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	defer server.Close()

	if err := client.CloseWithError(13); err != nil {
		t.Fatalf("CloseWithError: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := server.AcceptStream(); err != nil {
			var ge *GoAwayError
			if !errors.As(err, &ge) {
				t.Fatalf("AcceptStream err %v (%T), want *GoAwayError", err, err)
			}
			if ge.ErrorCode != 13 || !ge.Remote {
				t.Fatalf("GoAwayError{%d, remote=%v}, want {13, remote=true}", ge.ErrorCode, ge.Remote)
			}
			if !errors.Is(err, ErrRemoteGoAway) {
				t.Fatal("a remote GoAwayError must match ErrRemoteGoAway")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the go-away")
		}
	}
}

// TestHalfClose checks CloseWrite is visible as EOF to the peer while the
// receive half keeps working, and CloseRead stays local to this endpoint.
func TestHalfClose(t *testing.T) {
	client, server := pair(t, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		in, err := server.AcceptStream()
		if err != nil {
			t.Errorf("AcceptStream: %v", err)
			return
		}
		rest, err := io.ReadAll(in)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
			return
		}
		if string(rest) != "request" {
			t.Errorf("read %q, want %q", rest, "request")
			return
		}
		if _, err := in.Write([]byte("response")); err != nil {
			t.Errorf("write after peer FIN: %v", err)
		}
		in.Close()
	}()

	out, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := out.Write([]byte("request")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := out.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	back := make([]byte, len("response"))
	if _, err := io.ReadFull(out, back); err != nil {
		t.Fatalf("read after CloseWrite: %v", err)
	}
	if string(back) != "response" {
		t.Fatalf("read %q, want %q", back, "response")
	}
	<-done

	// CloseRead is local: reads stop, the write half is untouched.
	out2, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if err := out2.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}
	if _, err := out2.Read(make([]byte, 4)); !errors.Is(err, ErrStreamReset) {
		t.Fatalf("read after CloseRead = %v, want ErrStreamReset", err)
	}
	if _, err := out2.Write([]byte("still writable")); err != nil {
		t.Fatalf("write after CloseRead: %v", err)
	}
}

// countingManager records the reservations a session makes.
type countingManager struct {
	mu        sync.Mutex
	spans     int
	reserved  int
	released  int
	doneCalls int
	failNext  bool
}

func (m *countingManager) begin() (MemoryManager, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return nil, errors.New("memory limit reached")
	}
	m.spans++
	return (*countingSpan)(m), nil
}

type countingSpan countingManager

func (s *countingSpan) ReserveMemory(size int, prio uint8) error {
	m := (*countingManager)(s)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserved += size
	return nil
}

func (s *countingSpan) ReleaseMemory(size int) {
	m := (*countingManager)(s)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released += size
}

func (s *countingSpan) Done() {
	m := (*countingManager)(s)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doneCalls++
}

// TestMemoryManager checks a span is opened per stream and closed exactly once
// when the stream ends, and that a refused reservation fails the open.
func TestMemoryManager(t *testing.T) {
	mgr := &countingManager{}
	client, server := pair(t, mgr.begin)

	accepted := make(chan *Stream, 1)
	go func() {
		in, err := server.AcceptStream()
		if err != nil {
			t.Errorf("AcceptStream: %v", err)
			close(accepted)
			return
		}
		accepted <- in
	}()

	out, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	in, ok := <-accepted
	if !ok {
		t.Fatal("accept failed")
	}

	mgr.mu.Lock()
	spans, reserved := mgr.spans, mgr.reserved
	mgr.mu.Unlock()
	if spans != 2 {
		t.Fatalf("spans = %d, want 2 (one per endpoint)", spans)
	}
	if want := 2 * int(DefaultConfig().InitialStreamWindowSize); reserved != want {
		t.Fatalf("reserved = %d, want %d", reserved, want)
	}

	// Closing twice must still release once.
	out.Close()
	out.Close()
	in.Reset()
	mgr.mu.Lock()
	done, released := mgr.doneCalls, mgr.released
	mgr.mu.Unlock()
	if done != 2 {
		t.Fatalf("Done called %d times, want 2", done)
	}
	if released != reserved {
		t.Fatalf("released %d, reserved %d", released, reserved)
	}

	// A manager that refuses must fail the open rather than leak a stream.
	mgr.mu.Lock()
	mgr.failNext = true
	mgr.mu.Unlock()
	if _, err := client.OpenStream(testCtx(t)); err == nil {
		t.Fatal("OpenStream must fail when the memory manager refuses")
	}
}

// TestOpenStreamContext checks a cancelled context fails the open instead of
// blocking, and that the error is the context's own.
func TestOpenStreamContext(t *testing.T) {
	client, _ := pair(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.OpenStream(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenStream with a cancelled context = %v, want context.Canceled", err)
	}
}

// TestPingAndRTT checks Ping reports a round trip and RTT remembers it.
func TestPingAndRTT(t *testing.T) {
	client, _ := pair(t, nil)
	if client.RTT() != 0 {
		t.Fatal("RTT must be zero before the first ping")
	}
	if _, err := client.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if client.RTT() <= 0 {
		t.Fatal("RTT must be positive after a ping")
	}
}

// TestClosedSessionErrors checks the shutdown sentinels after Close.
func TestClosedSessionErrors(t *testing.T) {
	client, _ := pair(t, nil)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !client.IsClosed() {
		t.Fatal("IsClosed must be true after Close")
	}
	select {
	case <-client.CloseChan():
	case <-time.After(5 * time.Second):
		t.Fatal("CloseChan must be closed after Close")
	}
	_, err := client.OpenStream(testCtx(t))
	if !errors.Is(err, ErrSessionShutdown) {
		t.Fatalf("OpenStream after Close = %v, want ErrSessionShutdown", err)
	}
}

// TestVerifyConfig covers the rejections a caller can trip over.
func TestVerifyConfig(t *testing.T) {
	if err := VerifyConfig(nil); err == nil {
		t.Fatal("nil config must be rejected")
	}
	for name, mutate := range map[string]func(*Config){
		"zero AcceptBacklog":      func(c *Config) { c.AcceptBacklog = 0 },
		"zero MaxIncomingStreams": func(c *Config) { c.MaxIncomingStreams = 0 },
		"tiny initial window":     func(c *Config) { c.InitialStreamWindowSize = 1024 },
		"max below initial":       func(c *Config) { c.MaxStreamWindowSize = c.InitialStreamWindowSize - 1 },
		"tiny MaxMessageSize":     func(c *Config) { c.MaxMessageSize = 1 },
		"negative ReadBufSize":    func(c *Config) { c.ReadBufSize = -1 },
		"keepalive without interval": func(c *Config) {
			c.EnableKeepAlive = true
			c.KeepAliveInterval = 0
		},
	} {
		cfg := DefaultConfig()
		mutate(cfg)
		if err := VerifyConfig(cfg); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
	if err := VerifyConfig(DefaultConfig()); err != nil {
		t.Fatalf("DefaultConfig must verify: %v", err)
	}
}
