// Package v5replace checks that go-libp2p's yamux muxer, built against
// github.com/qiangli/yamux/v5 through a replace directive, works end to end.
//
// This is the drop-in gate for the swap Outpost performs. go-libp2p converts
// between its own defined types and yamux.Session, yamux.Stream and
// yamux.Config, so the package compiling here already proves the exported
// shapes line up; the tests then exercise the behaviour it relies on.
package v5replace

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2pyamux "github.com/libp2p/go-libp2p/p2p/muxer/yamux"
)

// muxedPair builds a client and server MuxedConn over an in-memory transport,
// through go-libp2p's multiplexer, with scope nil so no resource manager is
// involved.
func muxedPair(t *testing.T) (network.MuxedConn, network.MuxedConn) {
	t.Helper()
	cConn, sConn := net.Pipe()
	client, err := libp2pyamux.DefaultTransport.NewConn(cConn, false, nil)
	if err != nil {
		t.Fatalf("client NewConn: %v", err)
	}
	server, err := libp2pyamux.DefaultTransport.NewConn(sConn, true, nil)
	if err != nil {
		t.Fatalf("server NewConn: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestMuxedConnBothDirections opens a stream from each side and round-trips a
// payload larger than one stream window.
func TestMuxedConnBothDirections(t *testing.T) {
	client, server := muxedPair(t)
	payload := bytes.Repeat([]byte("libp2p-yamux-v5"), 40*1024) // ~600 KiB

	for _, dir := range []struct {
		name             string
		opener, accepter network.MuxedConn
	}{
		{"client-to-server", client, server},
		{"server-to-client", server, client},
	} {
		t.Run(dir.name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				in, err := dir.accepter.AcceptStream()
				if err != nil {
					t.Errorf("AcceptStream: %v", err)
					return
				}
				defer in.Close()
				if _, err := io.Copy(in, io.LimitReader(in, int64(len(payload)))); err != nil {
					t.Errorf("echo: %v", err)
				}
			}()

			out, err := dir.opener.OpenStream(testCtx(t))
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
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
			<-done
		})
	}
}

// TestMuxedStreamResetError checks a stream error code reaches the peer as a
// network.StreamError, which is the mapping go-libp2p's parseError performs
// over our StreamError.
func TestMuxedStreamResetError(t *testing.T) {
	client, server := muxedPair(t)

	got := make(chan error, 1)
	go func() {
		in, err := server.AcceptStream()
		if err != nil {
			got <- err
			return
		}
		buf := make([]byte, 64)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := in.Read(buf); err != nil {
				got <- err
				return
			}
		}
		got <- nil
	}()

	out, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := out.Write([]byte("hi")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := out.ResetWithError(network.StreamErrorCode(55)); err != nil {
		t.Fatalf("ResetWithError: %v", err)
	}

	select {
	case err := <-got:
		var se *network.StreamError
		if !errors.As(err, &se) {
			t.Fatalf("peer error %v (%T), want *network.StreamError", err, err)
		}
		if se.ErrorCode != network.StreamErrorCode(55) || !se.Remote {
			t.Fatalf("network.StreamError{%d, remote=%v}, want {55, remote=true}", se.ErrorCode, se.Remote)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the reset")
	}
}

// TestMuxedConnCloseWithError checks a connection error code reaches the peer
// as a network.ConnError, go-libp2p's mapping of our GoAwayError.
func TestMuxedConnCloseWithError(t *testing.T) {
	client, server := muxedPair(t)

	if err := client.CloseWithError(network.ConnErrorCode(21)); err != nil {
		t.Fatalf("CloseWithError: %v", err)
	}
	if !client.IsClosed() {
		t.Fatal("IsClosed must be true after CloseWithError")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := server.AcceptStream(); err != nil {
			var ce *network.ConnError
			if !errors.As(err, &ce) {
				t.Fatalf("AcceptStream err %v (%T), want *network.ConnError", err, err)
			}
			if ce.ErrorCode != network.ConnErrorCode(21) || !ce.Remote {
				t.Fatalf("network.ConnError{%d, remote=%v}, want {21, remote=true}", ce.ErrorCode, ce.Remote)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the go-away")
		}
	}
}

// TestMuxedStreamHalfClose checks CloseWrite is EOF to the peer while the
// receive half keeps working, which libp2p request/response streams rely on.
func TestMuxedStreamHalfClose(t *testing.T) {
	client, server := muxedPair(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		in, err := server.AcceptStream()
		if err != nil {
			t.Errorf("AcceptStream: %v", err)
			return
		}
		defer in.Close()
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
	}()

	out, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer out.Close()
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
}

// TestMemoryManagerScope checks the muxer's MemoryManager plumbing: go-libp2p
// passes a peer scope, which our session must drive through its own
// MemoryManager interface.
func TestMemoryManagerScope(t *testing.T) {
	cConn, sConn := net.Pipe()
	scope := &countingScope{}
	client, err := libp2pyamux.DefaultTransport.NewConn(cConn, false, scope)
	if err != nil {
		t.Fatalf("client NewConn: %v", err)
	}
	defer client.Close()
	server, err := libp2pyamux.DefaultTransport.NewConn(sConn, true, nil)
	if err != nil {
		t.Fatalf("server NewConn: %v", err)
	}
	defer server.Close()

	accepted := make(chan network.MuxedStream, 1)
	go func() {
		in, err := server.AcceptStream()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- in
	}()
	out, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, ok := <-accepted; !ok {
		t.Fatal("accept failed")
	}
	if n := scope.spans.Load(); n != 1 {
		t.Fatalf("BeginSpan called %d times, want 1", n)
	}
	if n := scope.reserved.Load(); n <= 0 {
		t.Fatalf("reserved %d bytes, want a positive reservation", n)
	}
	out.Close()
	deadline := time.Now().Add(5 * time.Second)
	for scope.done.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := scope.done.Load(); n != 1 {
		t.Fatalf("span Done called %d times, want 1", n)
	}
}
