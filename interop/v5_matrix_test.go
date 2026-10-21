package interop

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	stockv5 "github.com/libp2p/go-yamux/v5"
	ourv5 "github.com/qiangli/yamux/v5"
)

// The v5 matrix pairs github.com/qiangli/yamux/v5 against stock
// github.com/libp2p/go-yamux/v5 in both roles. It is the drop-in gate for
// `replace github.com/libp2p/go-yamux/v5 => github.com/qiangli/yamux/v5`.

func ourV5Config() *ourv5.Config {
	c := ourv5.DefaultConfig()
	c.LogOutput = io.Discard
	c.EnableKeepAlive = false
	return c
}

func stockV5Config() *stockv5.Config {
	c := stockv5.DefaultConfig()
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

// echoOnce reads exactly len(want) bytes, verifies them, and writes them back.
func echoOnce(t *testing.T, c net.Conn, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Errorf("echo read: %v", err)
		return
	}
	if payloadHash(got) != payloadHash(want) {
		t.Errorf("echo payload mismatch (%d bytes)", len(got))
		return
	}
	if _, err := c.Write(got); err != nil {
		t.Errorf("echo write: %v", err)
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

// roundTrip writes payload on c, reads it back, and compares hashes.
func roundTrip(t *testing.T, c net.Conn, payload []byte) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := c.Write(payload); err != nil {
			t.Errorf("round-trip write: %v", err)
		}
	}()
	back := make([]byte, len(payload))
	if _, err := io.ReadFull(c, back); err != nil {
		t.Fatalf("round-trip read: %v", err)
	}
	if payloadHash(back) != payloadHash(payload) {
		t.Fatalf("round-trip payload mismatch (%d bytes)", len(back))
	}
	<-done
}

// TestV5InteropOursClientStockServer drives our v5 client against a stock
// go-yamux/v5 server: stream open in both directions, a payload bigger than
// one window, half-close, and ResetWithError code propagation.
func TestV5InteropOursClientStockServer(t *testing.T) {
	cConn, sConn := net.Pipe()
	ours, err := ourv5.Client(cConn, ourV5Config(), nil)
	if err != nil {
		t.Fatalf("our v5 Client: %v", err)
	}
	defer ours.Close()
	stock, err := stockv5.Server(sConn, stockV5Config(), nil)
	if err != nil {
		t.Fatalf("stock v5 Server: %v", err)
	}
	defer stock.Close()

	payload := randomPayload(700 * 1024)

	// ours opens, stock accepts
	t.Run("ours-open-stock-accept", func(t *testing.T) {
		accepted := make(chan struct{})
		go func() {
			defer close(accepted)
			sStream, err := stock.AcceptStream()
			if err != nil {
				t.Errorf("stock AcceptStream: %v", err)
				return
			}
			defer sStream.Close()
			echoOnce(t, sStream, payload)
		}()
		cStream, err := ours.OpenStream(testCtx(t))
		if err != nil {
			t.Fatalf("our OpenStream: %v", err)
		}
		defer cStream.Close()
		roundTrip(t, cStream, payload)
		<-accepted
	})

	// stock opens, ours accepts
	t.Run("stock-open-ours-accept", func(t *testing.T) {
		accepted := make(chan struct{})
		go func() {
			defer close(accepted)
			oStream, err := ours.AcceptStream()
			if err != nil {
				t.Errorf("our AcceptStream: %v", err)
				return
			}
			defer oStream.Close()
			echoOnce(t, oStream, payload)
		}()
		sStream, err := stock.OpenStream(testCtx(t))
		if err != nil {
			t.Fatalf("stock OpenStream: %v", err)
		}
		defer sStream.Close()
		roundTrip(t, sStream, payload)
		<-accepted
	})

	// our ResetWithError code must reach the stock peer as a StreamError.
	t.Run("ours-reset-code-to-stock", func(t *testing.T) {
		got := make(chan error, 1)
		go func() {
			sStream, err := stock.AcceptStream()
			if err != nil {
				got <- err
				return
			}
			got <- readUntilError(sStream)
		}()
		cStream, err := ours.OpenStream(testCtx(t))
		if err != nil {
			t.Fatalf("our OpenStream: %v", err)
		}
		if _, err := cStream.Write([]byte("x")); err != nil {
			t.Fatalf("our Write: %v", err)
		}
		if err := cStream.ResetWithError(42); err != nil {
			t.Fatalf("our ResetWithError: %v", err)
		}
		select {
		case err := <-got:
			var se *stockv5.StreamError
			if !errors.As(err, &se) {
				t.Fatalf("stock peer error %v (%T), want *stockv5.StreamError", err, err)
			}
			if se.ErrorCode != 42 || !se.Remote {
				t.Fatalf("stock saw StreamError{%d, remote=%v}, want {42, remote=true}", se.ErrorCode, se.Remote)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for stock peer reset")
		}
	})

	// a stock ResetWithError code must reach us as our StreamError.
	t.Run("stock-reset-code-to-ours", func(t *testing.T) {
		got := make(chan error, 1)
		go func() {
			oStream, err := ours.AcceptStream()
			if err != nil {
				got <- err
				return
			}
			got <- readUntilError(oStream)
		}()
		sStream, err := stock.OpenStream(testCtx(t))
		if err != nil {
			t.Fatalf("stock OpenStream: %v", err)
		}
		if _, err := sStream.Write([]byte("x")); err != nil {
			t.Fatalf("stock Write: %v", err)
		}
		if err := sStream.ResetWithError(7); err != nil {
			t.Fatalf("stock ResetWithError: %v", err)
		}
		select {
		case err := <-got:
			var se *ourv5.StreamError
			if !errors.As(err, &se) {
				t.Fatalf("our error %v (%T), want *ourv5.StreamError", err, err)
			}
			if se.ErrorCode != 7 || !se.Remote {
				t.Fatalf("we saw StreamError{%d, remote=%v}, want {7, remote=true}", se.ErrorCode, se.Remote)
			}
			if !errors.Is(err, ourv5.ErrStreamReset) {
				t.Fatal("our StreamError must match ErrStreamReset")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for our reset")
		}
	})
}

// TestV5InteropStockClientOursServer is the mirror direction: stock client,
// our v5 server, plus half-close and ping.
func TestV5InteropStockClientOursServer(t *testing.T) {
	cConn, sConn := net.Pipe()
	stock, err := stockv5.Client(cConn, stockV5Config(), nil)
	if err != nil {
		t.Fatalf("stock v5 Client: %v", err)
	}
	defer stock.Close()
	ours, err := ourv5.Server(sConn, ourV5Config(), nil)
	if err != nil {
		t.Fatalf("our v5 Server: %v", err)
	}
	defer ours.Close()

	payload := randomPayload(300 * 1024)

	t.Run("stock-open-ours-accept", func(t *testing.T) {
		accepted := make(chan struct{})
		go func() {
			defer close(accepted)
			oStream, err := ours.AcceptStream()
			if err != nil {
				t.Errorf("our AcceptStream: %v", err)
				return
			}
			defer oStream.Close()
			echoOnce(t, oStream, payload)
		}()
		sStream, err := stock.OpenStream(testCtx(t))
		if err != nil {
			t.Fatalf("stock OpenStream: %v", err)
		}
		defer sStream.Close()
		roundTrip(t, sStream, payload)
		<-accepted
	})

	t.Run("ours-open-stock-accept", func(t *testing.T) {
		accepted := make(chan struct{})
		go func() {
			defer close(accepted)
			sStream, err := stock.AcceptStream()
			if err != nil {
				t.Errorf("stock AcceptStream: %v", err)
				return
			}
			defer sStream.Close()
			echoOnce(t, sStream, payload)
		}()
		oStream, err := ours.OpenStream(testCtx(t))
		if err != nil {
			t.Fatalf("our OpenStream: %v", err)
		}
		defer oStream.Close()
		roundTrip(t, oStream, payload)
		<-accepted
	})

	// half-close: our CloseWrite must surface as io.EOF on the stock peer
	// while our receive half stays usable.
	t.Run("ours-half-close-to-stock", func(t *testing.T) {
		reply := []byte("after-fin")
		done := make(chan struct{})
		go func() {
			defer close(done)
			sStream, err := stock.AcceptStream()
			if err != nil {
				t.Errorf("stock AcceptStream: %v", err)
				return
			}
			defer sStream.Close()
			rest, err := io.ReadAll(sStream)
			if err != nil {
				t.Errorf("stock ReadAll: %v", err)
				return
			}
			if !bytes.Equal(rest, []byte("ping")) {
				t.Errorf("stock read %q, want %q", rest, "ping")
				return
			}
			if _, err := sStream.Write(reply); err != nil {
				t.Errorf("stock write after FIN: %v", err)
			}
		}()
		oStream, err := ours.OpenStream(testCtx(t))
		if err != nil {
			t.Fatalf("our OpenStream: %v", err)
		}
		defer oStream.Close()
		if _, err := oStream.Write([]byte("ping")); err != nil {
			t.Fatalf("our Write: %v", err)
		}
		if err := oStream.CloseWrite(); err != nil {
			t.Fatalf("our CloseWrite: %v", err)
		}
		back := make([]byte, len(reply))
		if _, err := io.ReadFull(oStream, back); err != nil {
			t.Fatalf("our read after CloseWrite: %v", err)
		}
		if !bytes.Equal(back, reply) {
			t.Fatalf("our read %q, want %q", back, reply)
		}
		<-done
	})

	// ping/RTT must complete against the stock peer in both directions.
	t.Run("ping-both-directions", func(t *testing.T) {
		if _, err := ours.Ping(); err != nil {
			t.Fatalf("our Ping: %v", err)
		}
		if ours.RTT() <= 0 {
			t.Fatal("our RTT must be positive after a ping")
		}
		if _, err := stock.Ping(); err != nil {
			t.Fatalf("stock Ping: %v", err)
		}
	})
}

// TestV5InteropGoAwayCode checks that a stock CloseWithError code reaches us as
// a GoAwayError, and that ours reaches the stock peer the same way.
func TestV5InteropGoAwayCode(t *testing.T) {
	t.Run("stock-closes-with-error", func(t *testing.T) {
		cConn, sConn := net.Pipe()
		stock, err := stockv5.Client(cConn, stockV5Config(), nil)
		if err != nil {
			t.Fatalf("stock Client: %v", err)
		}
		ours, err := ourv5.Server(sConn, ourV5Config(), nil)
		if err != nil {
			t.Fatalf("our Server: %v", err)
		}
		defer ours.Close()

		if err := stock.CloseWithError(9); err != nil {
			t.Fatalf("stock CloseWithError: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, err := ours.AcceptStream()
			if err != nil {
				var ge *ourv5.GoAwayError
				if !errors.As(err, &ge) {
					t.Fatalf("our AcceptStream err %v (%T), want *ourv5.GoAwayError", err, err)
				}
				if ge.ErrorCode != 9 || !ge.Remote {
					t.Fatalf("our GoAwayError{%d, remote=%v}, want {9, remote=true}", ge.ErrorCode, ge.Remote)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for remote go-away")
			}
		}
	})

	t.Run("ours-closes-with-error", func(t *testing.T) {
		cConn, sConn := net.Pipe()
		ours, err := ourv5.Client(cConn, ourV5Config(), nil)
		if err != nil {
			t.Fatalf("our Client: %v", err)
		}
		stock, err := stockv5.Server(sConn, stockV5Config(), nil)
		if err != nil {
			t.Fatalf("stock Server: %v", err)
		}
		defer stock.Close()

		if err := ours.CloseWithError(11); err != nil {
			t.Fatalf("our CloseWithError: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, err := stock.AcceptStream()
			if err != nil {
				var ge *stockv5.GoAwayError
				if !errors.As(err, &ge) {
					t.Fatalf("stock AcceptStream err %v (%T), want *stockv5.GoAwayError", err, err)
				}
				if ge.ErrorCode != 11 || !ge.Remote {
					t.Fatalf("stock GoAwayError{%d, remote=%v}, want {11, remote=true}", ge.ErrorCode, ge.Remote)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for stock to see our go-away")
			}
		}
	})
}
