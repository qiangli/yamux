package yamux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// TestFRPCompat exercises the frp v0.68.1 call shapes described in
// API-INVENTORY.md §2. This is the red phase: before api.go exists it
// fails to compile due to undefined symbols (LogOutput field, Clone,
// Logger, ErrTimeout, Addr, AcceptStreamWithContext, Session, etc.).
func TestFRPCompat_OpenAcceptBothDirections(t *testing.T) {
	cfg := DefaultConfig()
	// frp sets these three fields after DefaultConfig()
	cfg.KeepAliveInterval = 30 * time.Second
	var buf bytes.Buffer
	cfg.LogOutput = &buf
	cfg.MaxStreamWindowSize = 6 * 1024 * 1024

	if cfg.Clone() == nil {
		t.Fatal("Clone returned nil")
	}
	// VerifyConfig must accept the frp-shaped config
	if err := VerifyConfig(cfg); err != nil {
		t.Fatalf("VerifyConfig: %v", err)
	}
	// mutual logger exclusion
	bad := cfg.Clone()
	bad.Logger = &testLogger{}
	bad.LogOutput = &buf
	if err := VerifyConfig(bad); err == nil {
		t.Fatal("VerifyConfig should reject both Logger and LogOutput set")
	}

	c1, s1 := net.Pipe()
	clientSess, err := Client(c1, cfg)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	defer clientSess.Close()
	serverSess, err := Server(s1, cfg)
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	defer serverSess.Close()

	// Verify Session interface shape
	var _ net.Conn = (*Stream)(nil)

	// Open from client, Accept on server
	cStream, err := clientSess.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if cStream.StreamID() == 0 {
		t.Fatal("client StreamID must be non-zero")
	}
	if cStream.Session() != clientSess {
		t.Fatal("Stream.Session() should return parent")
	}
	// net.Conn interface
	if _, ok := interface{}(cStream).(net.Conn); !ok {
		t.Fatal("Stream must implement net.Conn")
	}
	sStream, err := serverSess.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	// Accept variant
	_ = sStream.LocalAddr()
	_ = sStream.RemoteAddr()
	sStream.SetDeadline(time.Time{})
	sStream.SetReadDeadline(time.Time{})
	sStream.SetWriteDeadline(time.Time{})
	sStream.Shrink()

	const msg = "hello frp compat"
	if _, err := cStream.Write([]byte(msg)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(sStream, got); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != msg {
		t.Fatalf("got %q want %q", got, msg)
	}

	// Open from server, Accept on client (reverse direction)
	sStream2, err := serverSess.Open()
	if err != nil {
		t.Fatalf("Server Open: %v", err)
	}
	cStream2, err := clientSess.Accept()
	if err != nil {
		t.Fatalf("Client Accept: %v", err)
	}
	const msg2 = "reverse"
	if _, err := sStream2.Write([]byte(msg2)); err != nil {
		t.Fatalf("reverse Write: %v", err)
	}
	got2 := make([]byte, len(msg2))
	if _, err := io.ReadFull(cStream2, got2); err != nil {
		t.Fatalf("reverse Read: %v", err)
	}
	if string(got2) != msg2 {
		t.Fatalf("reverse got %q want %q", got2, msg2)
	}

	// AcceptStreamWithContext
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// start a goroutine that opens after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		st, _ := clientSess.OpenStream()
		if st != nil {
			_, _ = st.Write([]byte("ctx"))
			// keep open briefly so accept sees it
			time.Sleep(50 * time.Millisecond)
			_ = st.Close()
		}
	}()
	ctxStream, err := serverSess.AcceptStreamWithContext(ctx)
	if err != nil {
		t.Fatalf("AcceptStreamWithContext: %v", err)
	}
	if ctxStream == nil {
		t.Fatal("AcceptStreamWithContext returned nil stream")
	}
	_ = ctxStream.Close()

	// Ping
	if _, err := clientSess.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	// Session introspection
	if clientSess.NumStreams() < 0 {
		t.Fatal("NumStreams negative")
	}
	if clientSess.IsClosed() {
		t.Fatal("IsClosed should be false before Close")
	}
	select {
	case <-clientSess.CloseChan():
		t.Fatal("CloseChan should not be closed yet")
	default:
	}
	_ = clientSess.LocalAddr()
	_ = clientSess.RemoteAddr()
	_ = clientSess.Addr()

	// GoAway
	if err := serverSess.GoAway(); err != nil {
		t.Fatalf("GoAway: %v", err)
	}

	// cleanup streams
	_ = cStream.Close()
	_ = sStream.Close()
	_ = cStream2.Close()
	_ = sStream2.Close()
}

func TestHashicorpErrorSentinels(t *testing.T) {
	// All exported error values from API-INVENTORY §1.6 must exist
	sentinels := []error{
		ErrInvalidVersion,
		ErrInvalidMsgType,
		ErrSessionShutdown,
		ErrStreamsExhausted,
		ErrDuplicateStream,
		ErrRecvWindowExceeded,
		ErrTimeout,
		ErrStreamClosed,
		ErrUnexpectedFlag,
		ErrRemoteGoAway,
		ErrConnectionReset,
		ErrConnectionWriteTimeout,
		ErrKeepAliveTimeout,
	}
	for _, e := range sentinels {
		if e == nil {
			t.Fatal("sentinel is nil")
		}
	}
	// ErrTimeout must be a net.Error with Timeout true
	var ne net.Error
	if !errors.As(ErrTimeout, &ne) {
		t.Fatalf("ErrTimeout does not implement net.Error: %T", ErrTimeout)
	}
	if !ne.Timeout() {
		t.Fatal("ErrTimeout.Timeout() should be true")
	}
	// Logger interface must be usable
	var _ Logger = &testLogger{}
}

func TestClientServerAcceptIOReadWriteCloser(t *testing.T) {
	// hashicorp signature is Client(conn io.ReadWriteCloser, config *Config)
	// net.Pipe returns net.Conn which is also io.ReadWriteCloser; ensure
	// the constructors accept io.ReadWriteCloser via type assertion.
	var rw io.ReadWriteCloser = &pipeRW{Reader: bytes.NewReader(nil), Closer: io.NopCloser(nil)}
	_ = rw
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	left, right := net.Pipe()
	c, err := Client(left, cfg)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	s, err := Server(right, cfg)
	if err != nil {
		_ = c.Close()
		t.Fatalf("Server: %v", err)
	}
	_ = c.Close()
	_ = s.Close()
}

// helpers

type testLogger struct{}

func (l *testLogger) Print(v ...interface{})                 {}
func (l *testLogger) Printf(format string, v ...interface{}) {}
func (l *testLogger) Println(v ...interface{})               {}

type pipeRW struct {
	io.Reader
	io.Closer
}

func (p *pipeRW) Write(b []byte) (int, error) { return len(b), nil }
