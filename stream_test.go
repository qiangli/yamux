package yamux

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func TestStreamWindowBackpressure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	client, server := newTestSessionPair(t, cfg)
	defer closeSessionPair(t, client, server)

	writer, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	reader, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	payload := bytes.Repeat([]byte{'w'}, DefaultStreamWindow+1)
	type result struct {
		n   int64
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		n, copyErr := io.Copy(writer, bytes.NewReader(payload))
		resultCh <- result{n: n, err: copyErr}
	}()

	waitFor(t, "the sender to exhaust its stream window", func() bool {
		writer.mu.Lock()
		defer writer.mu.Unlock()
		return writer.sendWindow == 0
	})
	select {
	case got := <-resultCh:
		t.Fatalf("writer completed without a window update: n=%d err=%v", got.n, got.err)
	default:
	}

	one := make([]byte, 1)
	if _, err := io.ReadFull(reader, one); err != nil {
		t.Fatalf("read to release credit: %v", err)
	}
	select {
	case got := <-resultCh:
		if got.err != nil || got.n != int64(len(payload)) {
			t.Fatalf("writer result = (%d, %v), want (%d, nil)", got.n, got.err, len(payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer stayed blocked after a window update")
	}
	_ = writer.Reset()
	_ = reader.Reset()
}

func TestHalfCloseThenReset(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	client, server := newTestSessionPair(t, cfg)
	defer closeSessionPair(t, client, server)

	local, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	remote, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if err := local.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	buf := make([]byte, 1)
	if n, err := remote.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("read after FIN = (%d, %v), want (0, EOF)", n, err)
	}

	if err := local.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	waitFor(t, "remote reset", func() bool {
		remote.mu.Lock()
		defer remote.mu.Unlock()
		return remote.resetErr != nil
	})
	if n, err := remote.Read(buf); n != 0 || !errors.Is(err, ErrStreamReset) {
		t.Fatalf("read after RST = (%d, %v), want (0, %v)", n, err, ErrStreamReset)
	}
}

func TestStreamReadWriteDeadlines(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	client, server := newTestSessionPair(t, cfg)
	defer closeSessionPair(t, client, server)

	local, err := client.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	remote, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if err := remote.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 1)
	if n, err := remote.Read(buf); n != 0 || !isDeadlineError(err) {
		t.Fatalf("deadline read = (%d, %v), want timeout", n, err)
	}
	if err := remote.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}
	if _, err := local.Write([]byte{'x'}); err != nil {
		t.Fatalf("write after read timeout: %v", err)
	}
	if _, err := io.ReadFull(remote, buf); err != nil || buf[0] != 'x' {
		t.Fatalf("read after clearing deadline = (%q, %v)", buf, err)
	}

	if _, err := local.Write(make([]byte, DefaultStreamWindow)); err != nil {
		t.Fatalf("exhaust send window: %v", err)
	}
	if err := local.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if n, err := local.Write([]byte{'y'}); n != 0 || !isDeadlineError(err) {
		t.Fatalf("deadline write = (%d, %v), want timeout", n, err)
	}
	_ = local.Reset()
	_ = remote.Reset()
}

func isDeadlineError(err error) bool {
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return false
	}
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}
