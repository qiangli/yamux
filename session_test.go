package yamux

import (
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

func newTestSessionPair(t *testing.T, cfg *Config) (*Session, *Session) {
	t.Helper()
	left, right := net.Pipe()
	client, err := Client(left, cfg)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	server, err := Server(right, cfg)
	if err != nil {
		_ = client.Close()
		t.Fatalf("Server: %v", err)
	}
	return client, server
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func closeSessionPair(t *testing.T, a, b *Session) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = a.Close()
		close(done)
	}()
	_ = b.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sessions did not close")
	}
}

func assertGoroutinesReturn(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session goroutines leaked: before=%d after=%d", before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestGoAwayRejectsNewStreams(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.StreamCloseTimeout = 50 * time.Millisecond
	client, server := newTestSessionPair(t, cfg)

	existing, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	peer, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	if err := server.GoAway(); err != nil {
		t.Fatalf("GoAway: %v", err)
	}
	if got := server.State(); got != SessionClosing {
		t.Fatalf("state after GoAway = %v, want %v", got, SessionClosing)
	}
	waitFor(t, "remote go-away", func() bool {
		stream, openErr := client.OpenStream()
		if openErr == nil {
			_ = stream.Reset()
			return false
		}
		return errors.Is(openErr, ErrRemoteGoAway)
	})
	if _, err := server.OpenStream(); !errors.Is(err, ErrSessionShutdown) {
		t.Fatalf("local open after GoAway error = %v, want %v", err, ErrSessionShutdown)
	}

	const message = "existing stream still drains"
	if _, err := io.WriteString(existing, message); err != nil {
		t.Fatalf("write existing stream: %v", err)
	}
	got := make([]byte, len(message))
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatalf("read existing stream: %v", err)
	}
	if string(got) != message {
		t.Fatalf("read %q, want %q", got, message)
	}

	_ = existing.Reset()
	_ = peer.Reset()
	closeSessionPair(t, client, server)
	if client.State() != SessionClosed || server.State() != SessionClosed {
		t.Fatalf("states after Close = (%v, %v), want both Closed", client.State(), server.State())
	}
	assertGoroutinesReturn(t, before)
}

func TestPingRoundTrip(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	client, server := newTestSessionPair(t, cfg)
	defer closeSessionPair(t, client, server)
	rtt, err := client.Ping()
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if rtt <= 0 || client.RTT() != rtt {
		t.Fatalf("RTT = %v, recorded %v", rtt, client.RTT())
	}
}

func TestPingMatchesPongIdentifier(t *testing.T) {
	left, right := net.Pipe()
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	client, err := Client(left, cfg)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = right.Close()
	}()

	pingResult := make(chan error, 1)
	go func() {
		_, pingErr := client.Ping()
		pingResult <- pingErr
	}()
	var raw [HeaderSize]byte
	if _, err := io.ReadFull(right, raw[:]); err != nil {
		t.Fatalf("read ping: %v", err)
	}
	ping, err := DecodeHeader(raw[:])
	if err != nil {
		t.Fatalf("decode ping: %v", err)
	}
	if ping.Type != TypePing || ping.Flags != FlagSYN {
		t.Fatalf("got header %+v, want ping SYN", ping)
	}
	badPong := Header{Type: TypePing, Flags: FlagACK, Length: ping.Length + 1}.Encode()
	if _, err := right.Write(badPong[:]); err != nil {
		t.Fatalf("write mismatched pong: %v", err)
	}
	select {
	case err := <-pingResult:
		if err == nil || !strings.Contains(err.Error(), "unexpected pong") {
			t.Fatalf("Ping error = %v, want unexpected pong", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Ping did not reject mismatched pong")
	}
	if !client.IsClosed() {
		t.Fatal("session stayed active after mismatched pong")
	}
}
