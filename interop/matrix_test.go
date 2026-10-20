package interop

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	stockyamux "github.com/hashicorp/yamux"
	ouryamux "github.com/qiangli/yamux"
)

// TestOursClientVsStockServer tests our Client against stock hashicorp/yamux Server.
func TestOursClientVsStockServer(t *testing.T) {
	t.Run("OpenAccept", func(t *testing.T) {
		client, server, cleanup := setupOursClientStockServer(t, nil, nil)
		defer cleanup()

		// 1. Client opens stream -> Server accepts (odd stream ID: 1)
		sCh := make(chan *stockyamux.Stream, 1)
		errCh := make(chan error, 1)
		go func() {
			s, err := server.AcceptStream()
			if err != nil {
				errCh <- err
				return
			}
			sCh <- s
		}()

		cStream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("client.OpenStream: %v", err)
		}
		if cStream.StreamID() != 1 {
			t.Fatalf("expected client stream ID 1, got %d", cStream.StreamID())
		}
		if _, err := cStream.Write([]byte("c2s")); err != nil {
			t.Fatalf("cStream.Write: %v", err)
		}

		select {
		case err := <-errCh:
			t.Fatalf("server.AcceptStream: %v", err)
		case sStream := <-sCh:
			if sStream.StreamID() != 1 {
				t.Fatalf("expected server accepted stream ID 1, got %d", sStream.StreamID())
			}
			buf := make([]byte, 3)
			if _, err := io.ReadFull(sStream, buf); err != nil {
				t.Fatalf("sStream.ReadFull: %v", err)
			}
			if string(buf) != "c2s" {
				t.Fatalf("got %s, want c2s", string(buf))
			}
			_ = sStream.Close()
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for server.AcceptStream")
		}
		_ = cStream.Close()

		// 2. Server opens stream -> Client accepts (even stream ID: 2)
		cAcceptCh := make(chan *ouryamux.Stream, 1)
		cErrCh := make(chan error, 1)
		go func() {
			s, err := client.AcceptStream()
			if err != nil {
				cErrCh <- err
				return
			}
			cAcceptCh <- s
		}()

		sStream2, err := server.OpenStream()
		if err != nil {
			t.Fatalf("server.OpenStream: %v", err)
		}
		if sStream2.StreamID() != 2 {
			t.Fatalf("expected server stream ID 2, got %d", sStream2.StreamID())
		}
		if _, err := sStream2.Write([]byte("s2c")); err != nil {
			t.Fatalf("sStream2.Write: %v", err)
		}

		select {
		case err := <-cErrCh:
			t.Fatalf("client.AcceptStream: %v", err)
		case cStream2 := <-cAcceptCh:
			if cStream2.StreamID() != 2 {
				t.Fatalf("expected client accepted stream ID 2, got %d", cStream2.StreamID())
			}
			buf := make([]byte, 3)
			if _, err := io.ReadFull(cStream2, buf); err != nil {
				t.Fatalf("cStream2.ReadFull: %v", err)
			}
			if string(buf) != "s2c" {
				t.Fatalf("got %s, want s2c", string(buf))
			}
			_ = sStream2.Close()
			_ = cStream2.Close()
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for client.AcceptStream")
		}
	})

	t.Run("DataEcho", func(t *testing.T) {
		client, server, cleanup := setupOursClientStockServer(t, nil, nil)
		defer cleanup()

		// Server echo worker
		go func() {
			for {
				s, err := server.AcceptStream()
				if err != nil {
					return
				}
				go func(stream *stockyamux.Stream) {
					defer stream.Close()
					_, _ = io.Copy(stream, stream)
				}(s)
			}
		}()

		sizes := []int{1, 256, 1024, 16384, 65536}
		for _, size := range sizes {
			payload := randomPayload(size)
			cStream, err := client.OpenStream()
			if err != nil {
				t.Fatalf("size %d OpenStream: %v", size, err)
			}

			if _, err := cStream.Write(payload); err != nil {
				t.Fatalf("size %d Write: %v", size, err)
			}

			recv := make([]byte, size)
			if _, err := io.ReadFull(cStream, recv); err != nil {
				t.Fatalf("size %d ReadFull: %v", size, err)
			}
			if !bytes.Equal(recv, payload) {
				t.Fatalf("size %d payload mismatch", size)
			}
			_ = cStream.Close()
		}
	})

	t.Run("WindowUpdates_LargeTransfer", func(t *testing.T) {
		client, server, cleanup := setupOursClientStockServer(t, nil, nil)
		defer cleanup()

		// 2MB payload >> 256KB default stream window
		const payloadSize = 2 * 1024 * 1024
		payload := randomPayload(payloadSize)
		expectedHash := payloadHash(payload)

		// Full-duplex large transfer: client and server send 2MB to each other simultaneously
		errCh := make(chan error, 2)
		go func() {
			sStream, err := server.AcceptStream()
			if err != nil {
				errCh <- err
				return
			}
			defer sStream.Close()

			var wg sync.WaitGroup
			wg.Add(2)

			// Server writer
			go func() {
				defer wg.Done()
				if _, err := sStream.Write(payload); err != nil {
					errCh <- fmt.Errorf("server write: %w", err)
				}
			}()

			// Server reader
			go func() {
				defer wg.Done()
				recv := make([]byte, payloadSize)
				if _, err := io.ReadFull(sStream, recv); err != nil {
					errCh <- fmt.Errorf("server read: %w", err)
					return
				}
				if payloadHash(recv) != expectedHash {
					errCh <- errors.New("server received hash mismatch")
				}
			}()

			wg.Wait()
			errCh <- nil
		}()

		cStream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("client.OpenStream: %v", err)
		}
		defer cStream.Close()

		var wg sync.WaitGroup
		wg.Add(2)

		// Client writer
		go func() {
			defer wg.Done()
			if _, err := cStream.Write(payload); err != nil {
				t.Errorf("client write: %v", err)
			}
		}()

		// Client reader
		go func() {
			defer wg.Done()
			recv := make([]byte, payloadSize)
			if _, err := io.ReadFull(cStream, recv); err != nil {
				t.Errorf("client read: %v", err)
				return
			}
			if payloadHash(recv) != expectedHash {
				t.Errorf("client received hash mismatch")
			}
		}()

		wg.Wait()
		if err := <-errCh; err != nil {
			t.Fatalf("server side error: %v", err)
		}
	})

	t.Run("PingRoundTrip", func(t *testing.T) {
		client, server, cleanup := setupOursClientStockServer(t, nil, nil)
		defer cleanup()

		rtt, err := client.Ping()
		if err != nil {
			t.Fatalf("client.Ping: %v", err)
		}
		if rtt <= 0 {
			t.Errorf("expected positive client RTT, got %v", rtt)
		}

		rtt, err = server.Ping()
		if err != nil {
			t.Fatalf("server.Ping: %v", err)
		}
		if rtt <= 0 {
			t.Errorf("expected positive server RTT, got %v", rtt)
		}
	})

	t.Run("GoAway", func(t *testing.T) {
		// Case A: Client sends GoAway -> Server cannot open new stream
		clientA, serverA, cleanupA := setupOursClientStockServer(t, nil, nil)
		defer cleanupA()

		sCh := make(chan *stockyamux.Stream, 1)
		go func() {
			s, _ := serverA.AcceptStream()
			sCh <- s
		}()
		cActive, err := clientA.OpenStream()
		if err != nil {
			t.Fatalf("client OpenStream: %v", err)
		}
		if _, err := cActive.Write([]byte("before-goaway")); err != nil {
			t.Fatalf("cActive.Write: %v", err)
		}
		sActive := <-sCh

		// Client sends GoAway
		if err := clientA.GoAway(); err != nil {
			t.Fatalf("client.GoAway: %v", err)
		}
		time.Sleep(30 * time.Millisecond)

		// Server cannot open new stream
		newStream, err := serverA.OpenStream()
		if err == nil {
			newStream.Close()
			t.Fatal("expected server OpenStream to fail after client GoAway")
		}

		// Existing stream can still communicate
		buf := make([]byte, 13)
		if _, err := io.ReadFull(sActive, buf); err != nil {
			t.Fatalf("sActive.ReadFull: %v", err)
		}
		if string(buf) != "before-goaway" {
			t.Fatalf("got %s, want before-goaway", string(buf))
		}
		_ = sActive.Close()
		_ = cActive.Close()

		// Case B: Server sends GoAway -> Client cannot open new stream
		clientB, serverB, cleanupB := setupOursClientStockServer(t, nil, nil)
		defer cleanupB()

		sChB := make(chan *stockyamux.Stream, 1)
		go func() {
			s, _ := serverB.AcceptStream()
			sChB <- s
		}()
		cActiveB, err := clientB.OpenStream()
		if err != nil {
			t.Fatalf("client OpenStream B: %v", err)
		}
		if _, err := cActiveB.Write([]byte("before-goaway-b")); err != nil {
			t.Fatalf("cActiveB.Write: %v", err)
		}
		sActiveB := <-sChB

		// Server sends GoAway
		if err := serverB.GoAway(); err != nil {
			t.Fatalf("server.GoAway: %v", err)
		}
		time.Sleep(30 * time.Millisecond)

		// Client cannot open new stream
		newStreamB, err := clientB.OpenStream()
		if err == nil {
			newStreamB.Close()
			t.Fatal("expected client OpenStream to fail after server GoAway")
		}

		// Existing stream B can still communicate
		bufB := make([]byte, 15)
		if _, err := io.ReadFull(sActiveB, bufB); err != nil {
			t.Fatalf("sActiveB.ReadFull: %v", err)
		}
		if string(bufB) != "before-goaway-b" {
			t.Fatalf("got %s, want before-goaway-b", string(bufB))
		}
		_ = sActiveB.Close()
		_ = cActiveB.Close()
	})

	t.Run("HalfCloseFIN", func(t *testing.T) {
		client, server, cleanup := setupOursClientStockServer(t, nil, nil)
		defer cleanup()

		sCh := make(chan *stockyamux.Stream, 1)
		go func() {
			s, _ := server.AcceptStream()
			sCh <- s
		}()

		cStream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}

		// Client writes request, then sends FIN via CloseWrite()
		req := []byte("hello-from-client")
		if _, err := cStream.Write(req); err != nil {
			t.Fatalf("cStream.Write: %v", err)
		}
		if err := cStream.CloseWrite(); err != nil {
			t.Fatalf("cStream.CloseWrite: %v", err)
		}

		sStream := <-sCh
		defer sStream.Close()

		// Server reads until EOF
		recvReq, err := io.ReadAll(sStream)
		if err != nil {
			t.Fatalf("server ReadAll: %v", err)
		}
		if !bytes.Equal(recvReq, req) {
			t.Fatalf("server got %s, want %s", recvReq, req)
		}

		// Server now writes response and closes (sends FIN)
		resp := []byte("reply-from-server")
		if _, err := sStream.Write(resp); err != nil {
			t.Fatalf("server Write: %v", err)
		}
		if err := sStream.Close(); err != nil {
			t.Fatalf("server Close: %v", err)
		}

		// Client reads until EOF
		recvResp, err := io.ReadAll(cStream)
		if err != nil {
			t.Fatalf("client ReadAll: %v", err)
		}
		if !bytes.Equal(recvResp, resp) {
			t.Fatalf("client got %s, want %s", recvResp, resp)
		}
		_ = cStream.Close()
	})

	t.Run("ResetRST", func(t *testing.T) {
		// Subtest 1: Our client resets stream -> Stock server sees connection reset
		t.Run("OursResets", func(t *testing.T) {
			client, server, cleanup := setupOursClientStockServer(t, nil, nil)
			defer cleanup()

			sCh := make(chan *stockyamux.Stream, 1)
			go func() {
				s, _ := server.AcceptStream()
				sCh <- s
			}()

			cStream, err := client.OpenStream()
			if err != nil {
				t.Fatalf("client.OpenStream: %v", err)
			}
			_, _ = cStream.Write([]byte("x"))

			sStream := <-sCh

			// Our client resets the stream
			if err := cStream.Reset(); err != nil {
				t.Fatalf("cStream.Reset: %v", err)
			}

			// Stock server stream should report connection reset on Read or Write
			buf := make([]byte, 10)
			_, rErr := sStream.Read(buf)
			if rErr == nil {
				_, wErr := sStream.Write([]byte("data"))
				if wErr == nil {
					t.Fatal("expected error on stock server stream after reset")
				}
			}
		})

		// Subtest 2: Stock server resets stream via StreamCloseTimeout -> Our client sees reset
		t.Run("StockResets", func(t *testing.T) {
			stockCfg := stockyamux.DefaultConfig()
			stockCfg.StreamCloseTimeout = 50 * time.Millisecond

			client, server, cleanup := setupOursClientStockServer(t, nil, stockCfg)
			defer cleanup()

			sCh := make(chan *stockyamux.Stream, 1)
			go func() {
				s, _ := server.AcceptStream()
				sCh <- s
			}()

			cStream, err := client.OpenStream()
			if err != nil {
				t.Fatalf("client.OpenStream: %v", err)
			}
			_, _ = cStream.Write([]byte("x"))

			sStream := <-sCh
			b := make([]byte, 1)
			_, _ = sStream.Read(b)

			// Stock server closes stream; client holds it open
			_ = sStream.Close()
			// Wait for stock StreamCloseTimeout to send RST
			time.Sleep(100 * time.Millisecond)

			buf := make([]byte, 10)
			_, rErr := cStream.Read(buf)
			if !errors.Is(rErr, ouryamux.ErrStreamReset) {
				_, wErr := cStream.Write([]byte("data"))
				if !errors.Is(wErr, ouryamux.ErrStreamReset) && !errors.Is(rErr, ouryamux.ErrStreamReset) {
					t.Fatalf("expected ErrStreamReset on client stream, got rErr=%v wErr=%v", rErr, wErr)
				}
			}
		})
	})

	t.Run("ConcurrentStreams200", func(t *testing.T) {
		ourCfg := ouryamux.DefaultConfig()
		ourCfg.AcceptBacklog = 256
		stockCfg := stockyamux.DefaultConfig()
		stockCfg.AcceptBacklog = 256

		client, server, cleanup := setupOursClientStockServer(t, ourCfg, stockCfg)
		defer cleanup()

		const count = 200
		var serverWg sync.WaitGroup
		serverWg.Add(count)

		// Server accepts 200 streams and echoes
		go func() {
			for i := 0; i < count; i++ {
				s, err := server.AcceptStream()
				if err != nil {
					return
				}
				go func(stream *stockyamux.Stream) {
					defer serverWg.Done()
					defer stream.Close()
					buf := make([]byte, 128)
					n, err := stream.Read(buf)
					if err != nil && err != io.EOF {
						return
					}
					_, _ = stream.Write(buf[:n])
				}(s)
			}
		}()

		var clientWg sync.WaitGroup
		clientWg.Add(count)
		startBarrier := make(chan struct{})

		// Client opens 200 streams concurrently
		for i := 0; i < count; i++ {
			go func(idx int) {
				defer clientWg.Done()
				cStream, err := client.OpenStream()
				if err != nil {
					t.Errorf("OpenStream[%d]: %v", idx, err)
					return
				}
				defer cStream.Close()

				<-startBarrier

				msg := []byte(fmt.Sprintf("concurrent-payload-%04d", idx))
				if _, err := cStream.Write(msg); err != nil {
					t.Errorf("Write[%d]: %v", idx, err)
					return
				}

				resp := make([]byte, len(msg))
				if _, err := io.ReadFull(cStream, resp); err != nil {
					t.Errorf("ReadFull[%d]: %v", idx, err)
					return
				}
				if !bytes.Equal(resp, msg) {
					t.Errorf("[%d] mismatch: got %s, want %s", idx, resp, msg)
				}
			}(i)
		}

		close(startBarrier)
		clientWg.Wait()
		serverWg.Wait()
	})
}

// TestStockClientVsOursServer tests stock hashicorp/yamux Client against our Server.
func TestStockClientVsOursServer(t *testing.T) {
	t.Run("OpenAccept", func(t *testing.T) {
		client, server, cleanup := setupStockClientOursServer(t, nil, nil)
		defer cleanup()

		// 1. Stock client opens stream -> Our server accepts (odd stream ID: 1)
		sCh := make(chan *ouryamux.Stream, 1)
		errCh := make(chan error, 1)
		go func() {
			s, err := server.AcceptStream()
			if err != nil {
				errCh <- err
				return
			}
			sCh <- s
		}()

		cStream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("client.OpenStream: %v", err)
		}
		if cStream.StreamID() != 1 {
			t.Fatalf("expected client stream ID 1, got %d", cStream.StreamID())
		}
		if _, err := cStream.Write([]byte("c2s")); err != nil {
			t.Fatalf("cStream.Write: %v", err)
		}

		select {
		case err := <-errCh:
			t.Fatalf("server.AcceptStream: %v", err)
		case sStream := <-sCh:
			if sStream.StreamID() != 1 {
				t.Fatalf("expected server accepted stream ID 1, got %d", sStream.StreamID())
			}
			buf := make([]byte, 3)
			if _, err := io.ReadFull(sStream, buf); err != nil {
				t.Fatalf("sStream.ReadFull: %v", err)
			}
			if string(buf) != "c2s" {
				t.Fatalf("got %s, want c2s", string(buf))
			}
			_ = cStream.Close()
			_ = sStream.Close()
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for server.AcceptStream")
		}

		// 2. Our server opens stream -> Stock client accepts (even stream ID: 2)
		cAcceptCh := make(chan *stockyamux.Stream, 1)
		cErrCh := make(chan error, 1)
		go func() {
			s, err := client.AcceptStream()
			if err != nil {
				cErrCh <- err
				return
			}
			cAcceptCh <- s
		}()

		sStream2, err := server.OpenStream()
		if err != nil {
			t.Fatalf("server.OpenStream: %v", err)
		}
		if sStream2.StreamID() != 2 {
			t.Fatalf("expected server stream ID 2, got %d", sStream2.StreamID())
		}
		if _, err := sStream2.Write([]byte("s2c")); err != nil {
			t.Fatalf("sStream2.Write: %v", err)
		}

		select {
		case err := <-cErrCh:
			t.Fatalf("client.AcceptStream: %v", err)
		case cStream2 := <-cAcceptCh:
			if cStream2.StreamID() != 2 {
				t.Fatalf("expected client accepted stream ID 2, got %d", cStream2.StreamID())
			}
			buf := make([]byte, 3)
			if _, err := io.ReadFull(cStream2, buf); err != nil {
				t.Fatalf("cStream2.ReadFull: %v", err)
			}
			if string(buf) != "s2c" {
				t.Fatalf("got %s, want s2c", string(buf))
			}
			_ = cStream2.Close()
			_ = sStream2.Close()
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for client.AcceptStream")
		}
	})

	t.Run("DataEcho", func(t *testing.T) {
		client, server, cleanup := setupStockClientOursServer(t, nil, nil)
		defer cleanup()

		// Our server echo worker
		go func() {
			for {
				s, err := server.AcceptStream()
				if err != nil {
					return
				}
				go func(stream *ouryamux.Stream) {
					defer stream.Close()
					_, _ = io.Copy(stream, stream)
				}(s)
			}
		}()

		sizes := []int{1, 256, 1024, 16384, 65536}
		for _, size := range sizes {
			payload := randomPayload(size)
			cStream, err := client.OpenStream()
			if err != nil {
				t.Fatalf("size %d OpenStream: %v", size, err)
			}

			if _, err := cStream.Write(payload); err != nil {
				t.Fatalf("size %d Write: %v", size, err)
			}

			recv := make([]byte, size)
			if _, err := io.ReadFull(cStream, recv); err != nil {
				t.Fatalf("size %d ReadFull: %v", size, err)
			}
			if !bytes.Equal(recv, payload) {
				t.Fatalf("size %d payload mismatch", size)
			}
			_ = cStream.Close()
		}
	})

	t.Run("WindowUpdates_LargeTransfer", func(t *testing.T) {
		client, server, cleanup := setupStockClientOursServer(t, nil, nil)
		defer cleanup()

		const payloadSize = 2 * 1024 * 1024
		payload := randomPayload(payloadSize)
		expectedHash := payloadHash(payload)

		errCh := make(chan error, 2)
		go func() {
			sStream, err := server.AcceptStream()
			if err != nil {
				errCh <- err
				return
			}
			defer sStream.Close()

			var wg sync.WaitGroup
			wg.Add(2)

			// Server writer
			go func() {
				defer wg.Done()
				if _, err := sStream.Write(payload); err != nil {
					errCh <- fmt.Errorf("server write: %w", err)
				}
			}()

			// Server reader
			go func() {
				defer wg.Done()
				recv := make([]byte, payloadSize)
				if _, err := io.ReadFull(sStream, recv); err != nil {
					errCh <- fmt.Errorf("server read: %w", err)
					return
				}
				if payloadHash(recv) != expectedHash {
					errCh <- errors.New("server received hash mismatch")
				}
			}()

			wg.Wait()
			errCh <- nil
		}()

		cStream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("client.OpenStream: %v", err)
		}
		defer cStream.Close()

		var wg sync.WaitGroup
		wg.Add(2)

		// Client writer
		go func() {
			defer wg.Done()
			if _, err := cStream.Write(payload); err != nil {
				t.Errorf("client write: %v", err)
			}
		}()

		// Client reader
		go func() {
			defer wg.Done()
			recv := make([]byte, payloadSize)
			if _, err := io.ReadFull(cStream, recv); err != nil {
				t.Errorf("client read: %v", err)
				return
			}
			if payloadHash(recv) != expectedHash {
				t.Errorf("client received hash mismatch")
			}
		}()

		wg.Wait()
		if err := <-errCh; err != nil {
			t.Fatalf("server side error: %v", err)
		}
	})

	t.Run("PingRoundTrip", func(t *testing.T) {
		client, server, cleanup := setupStockClientOursServer(t, nil, nil)
		defer cleanup()

		rtt, err := client.Ping()
		if err != nil {
			t.Fatalf("client.Ping: %v", err)
		}
		if rtt <= 0 {
			t.Errorf("expected positive client RTT, got %v", rtt)
		}

		rtt, err = server.Ping()
		if err != nil {
			t.Fatalf("server.Ping: %v", err)
		}
		if rtt <= 0 {
			t.Errorf("expected positive server RTT, got %v", rtt)
		}
	})

	t.Run("GoAway", func(t *testing.T) {
		// Case A: Stock Client sends GoAway -> Our Server cannot open new stream
		clientA, serverA, cleanupA := setupStockClientOursServer(t, nil, nil)
		defer cleanupA()

		sCh := make(chan *ouryamux.Stream, 1)
		go func() {
			s, _ := serverA.AcceptStream()
			sCh <- s
		}()
		cActive, err := clientA.OpenStream()
		if err != nil {
			t.Fatalf("client OpenStream: %v", err)
		}
		if _, err := cActive.Write([]byte("before-goaway")); err != nil {
			t.Fatalf("cActive.Write: %v", err)
		}
		sActive := <-sCh

		// Stock client sends GoAway
		if err := clientA.GoAway(); err != nil {
			t.Fatalf("client.GoAway: %v", err)
		}
		time.Sleep(30 * time.Millisecond)

		// Our server cannot open new stream
		newStream, err := serverA.OpenStream()
		if err == nil {
			newStream.Close()
			t.Fatal("expected server OpenStream to fail after client GoAway")
		}

		// Existing stream can still communicate
		buf := make([]byte, 13)
		if _, err := io.ReadFull(sActive, buf); err != nil {
			t.Fatalf("sActive.ReadFull: %v", err)
		}
		if string(buf) != "before-goaway" {
			t.Fatalf("got %s, want before-goaway", string(buf))
		}
		_ = cActive.Close()
		_ = sActive.Close()

		// Case B: Our Server sends GoAway -> Stock Client cannot open new stream
		clientB, serverB, cleanupB := setupStockClientOursServer(t, nil, nil)
		defer cleanupB()

		sChB := make(chan *ouryamux.Stream, 1)
		go func() {
			s, _ := serverB.AcceptStream()
			sChB <- s
		}()
		cActiveB, err := clientB.OpenStream()
		if err != nil {
			t.Fatalf("client OpenStream B: %v", err)
		}
		if _, err := cActiveB.Write([]byte("before-goaway-b")); err != nil {
			t.Fatalf("cActiveB.Write: %v", err)
		}
		sActiveB := <-sChB

		// Our server sends GoAway
		if err := serverB.GoAway(); err != nil {
			t.Fatalf("server.GoAway: %v", err)
		}
		time.Sleep(30 * time.Millisecond)

		// Stock client cannot open new stream
		newStreamB, err := clientB.OpenStream()
		if err == nil {
			newStreamB.Close()
			t.Fatal("expected client OpenStream to fail after server GoAway")
		}

		// Existing stream B can still communicate
		bufB := make([]byte, 15)
		if _, err := io.ReadFull(sActiveB, bufB); err != nil {
			t.Fatalf("sActiveB.ReadFull: %v", err)
		}
		if string(bufB) != "before-goaway-b" {
			t.Fatalf("got %s, want before-goaway-b", string(bufB))
		}
		_ = cActiveB.Close()
		_ = sActiveB.Close()
	})

	t.Run("HalfCloseFIN", func(t *testing.T) {
		client, server, cleanup := setupStockClientOursServer(t, nil, nil)
		defer cleanup()

		sCh := make(chan *ouryamux.Stream, 1)
		go func() {
			s, _ := server.AcceptStream()
			sCh <- s
		}()

		cStream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}

		// Stock client writes request, then calls Close() (sends FIN)
		req := []byte("hello-from-stock-client")
		if _, err := cStream.Write(req); err != nil {
			t.Fatalf("cStream.Write: %v", err)
		}
		if err := cStream.Close(); err != nil {
			t.Fatalf("cStream.Close: %v", err)
		}

		sStream := <-sCh
		defer sStream.Close()

		// Our server reads request until EOF
		recvReq, err := io.ReadAll(sStream)
		if err != nil {
			t.Fatalf("server ReadAll: %v", err)
		}
		if !bytes.Equal(recvReq, req) {
			t.Fatalf("server got %s, want %s", recvReq, req)
		}

		// Our server now writes response and half-closes write half via CloseWrite()
		resp := []byte("reply-from-our-server")
		if _, err := sStream.Write(resp); err != nil {
			t.Fatalf("server Write: %v", err)
		}
		if err := sStream.CloseWrite(); err != nil {
			t.Fatalf("server CloseWrite: %v", err)
		}

		// Stock client reads response until EOF
		recvResp, err := io.ReadAll(cStream)
		if err != nil {
			t.Fatalf("client ReadAll: %v", err)
		}
		if !bytes.Equal(recvResp, resp) {
			t.Fatalf("client got %s, want %s", recvResp, resp)
		}
	})

	t.Run("ResetRST", func(t *testing.T) {
		// Subtest 1: Our server resets stream -> Stock client sees connection reset
		t.Run("OursResets", func(t *testing.T) {
			client, server, cleanup := setupStockClientOursServer(t, nil, nil)
			defer cleanup()

			sCh := make(chan *ouryamux.Stream, 1)
			go func() {
				s, _ := server.AcceptStream()
				sCh <- s
			}()

			cStream, err := client.OpenStream()
			if err != nil {
				t.Fatalf("client.OpenStream: %v", err)
			}
			_, _ = cStream.Write([]byte("x"))

			sStream := <-sCh

			// Our server resets the stream
			if err := sStream.Reset(); err != nil {
				t.Fatalf("sStream.Reset: %v", err)
			}

			// Stock client stream should report connection reset on Read or Write
			buf := make([]byte, 10)
			_, rErr := cStream.Read(buf)
			if rErr == nil {
				_, wErr := cStream.Write([]byte("data"))
				if wErr == nil {
					t.Fatal("expected error on stock client stream after reset")
				}
			}
		})

		// Subtest 2: Stock client resets stream via StreamCloseTimeout -> Our server sees reset
		t.Run("StockResets", func(t *testing.T) {
			stockCfg := stockyamux.DefaultConfig()
			stockCfg.StreamCloseTimeout = 50 * time.Millisecond

			client, server, cleanup := setupStockClientOursServer(t, stockCfg, nil)
			defer cleanup()

			sCh := make(chan *ouryamux.Stream, 1)
			go func() {
				s, _ := server.AcceptStream()
				sCh <- s
			}()

			cStream, err := client.OpenStream()
			if err != nil {
				t.Fatalf("client.OpenStream: %v", err)
			}
			_, _ = cStream.Write([]byte("x"))

			sStream := <-sCh
			b := make([]byte, 1)
			_, _ = sStream.Read(b)

			// Stock client closes stream; server holds it open
			_ = cStream.Close()
			// Wait for stock StreamCloseTimeout to send RST
			time.Sleep(100 * time.Millisecond)

			buf := make([]byte, 10)
			_, rErr := sStream.Read(buf)
			if !errors.Is(rErr, ouryamux.ErrStreamReset) {
				_, wErr := sStream.Write([]byte("data"))
				if !errors.Is(wErr, ouryamux.ErrStreamReset) && !errors.Is(rErr, ouryamux.ErrStreamReset) {
					t.Fatalf("expected ErrStreamReset on server stream, got rErr=%v wErr=%v", rErr, wErr)
				}
			}
		})
	})

	t.Run("ConcurrentStreams200", func(t *testing.T) {
		stockCfg := stockyamux.DefaultConfig()
		stockCfg.AcceptBacklog = 256
		ourCfg := ouryamux.DefaultConfig()
		ourCfg.AcceptBacklog = 256

		client, server, cleanup := setupStockClientOursServer(t, stockCfg, ourCfg)
		defer cleanup()

		const count = 200

		var serverWg sync.WaitGroup
		serverWg.Add(count)

		// Our server accepts 200 streams and echoes
		go func() {
			for i := 0; i < count; i++ {
				s, err := server.AcceptStream()
				if err != nil {
					return
				}
				go func(stream *ouryamux.Stream) {
					defer serverWg.Done()
					defer stream.Close()
					buf := make([]byte, 128)
					n, err := stream.Read(buf)
					if err != nil && err != io.EOF {
						return
					}
					_, _ = stream.Write(buf[:n])
				}(s)
			}
		}()

		var openMu sync.Mutex
		var clientWg sync.WaitGroup
		clientWg.Add(count)
		startBarrier := make(chan struct{})

		// Stock client opens 200 streams. Synchronize OpenStream to avoid stock yamux emitting out-of-order SYN frames.
		for i := 0; i < count; i++ {
			go func(idx int) {
				defer clientWg.Done()

				openMu.Lock()
				cStream, err := client.OpenStream()
				openMu.Unlock()

				if err != nil {
					t.Errorf("OpenStream[%d]: %v", idx, err)
					return
				}
				defer cStream.Close()

				// All 200 streams concurrently write, read, and close
				<-startBarrier

				msg := []byte(fmt.Sprintf("concurrent-payload-%04d", idx))
				if _, err := cStream.Write(msg); err != nil {
					t.Errorf("Write[%d]: %v", idx, err)
					return
				}

				resp := make([]byte, len(msg))
				if _, err := io.ReadFull(cStream, resp); err != nil {
					t.Errorf("ReadFull[%d]: %v", idx, err)
					return
				}
				if !bytes.Equal(resp, msg) {
					t.Errorf("[%d] mismatch: got %s, want %s", idx, resp, msg)
				}
			}(i)
		}

		close(startBarrier)
		clientWg.Wait()
		serverWg.Wait()
	})
}
