package interop

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	stockyamux "github.com/hashicorp/yamux"
	ouryamux "github.com/qiangli/yamux"
)

// TestSoak runs a ~5-minute continuous soak test in both directions:
// (1) Ours Client vs Stock Server
// (2) Stock Client vs Ours Server
// Exercising stream open/close cycles, small and large payloads (> window),
// keepalive pings, and session cycling without deadlocks, leaks, or race conditions.
func TestSoak(t *testing.T) {
	duration := 5 * time.Minute
	if v := os.Getenv("SOAK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			duration = d
		}
	} else if testing.Short() {
		duration = 5 * time.Second
	}

	t.Logf("Starting Soak test with duration: %v (testing.Short=%v)", duration, testing.Short())

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var (
		totalStreams uint64
		totalBytes   uint64
		totalPings   uint64
		totalCycles  uint64
	)

	// Pre-generate sample payloads of varying sizes
	smallPayload := randomPayload(256)
	medPayload := randomPayload(16 * 1024)
	largePayload := randomPayload(512 * 1024) // > 256KB default window

	payloads := [][]byte{smallPayload, medPayload, largePayload}

	var wg sync.WaitGroup

	// Runner for direction 1: Our Client -> Stock Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		runSoakDirection(ctx, t, "OursClient_StockServer", true, payloads, &totalStreams, &totalBytes, &totalPings, &totalCycles)
	}()

	// Runner for direction 2: Stock Client -> Our Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		runSoakDirection(ctx, t, "StockClient_OursServer", false, payloads, &totalStreams, &totalBytes, &totalPings, &totalCycles)
	}()

	// Periodic progress logger
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

progressLoop:
	for {
		select {
		case <-done:
			break progressLoop
		case <-ticker.C:
			elapsed := time.Since(start).Round(time.Second)
			t.Logf("Soak running: %v / %v elapsed | streams: %d | bytes: %.2f MB | pings: %d | session cycles: %d",
				elapsed, duration,
				atomic.LoadUint64(&totalStreams),
				float64(atomic.LoadUint64(&totalBytes))/(1024*1024),
				atomic.LoadUint64(&totalPings),
				atomic.LoadUint64(&totalCycles),
			)
		}
	}

	elapsed := time.Since(start)
	t.Logf("Soak completed in %v: streams=%d, data=%.2f MB, pings=%d, session cycles=%d",
		elapsed,
		atomic.LoadUint64(&totalStreams),
		float64(atomic.LoadUint64(&totalBytes))/(1024*1024),
		atomic.LoadUint64(&totalPings),
		atomic.LoadUint64(&totalCycles),
	)

	if atomic.LoadUint64(&totalStreams) == 0 {
		t.Fatal("soak test did not open any streams")
	}
}

func runSoakDirection(
	ctx context.Context,
	t *testing.T,
	label string,
	oursIsClient bool,
	payloads [][]byte,
	totalStreams, totalBytes, totalPings, totalCycles *uint64,
) {
	for ctx.Err() == nil {
		// Create session pair
		var (
			clientOpenStream func() (io.ReadWriteCloser, error)
			serverAccept     func() (io.ReadWriteCloser, error)
			clientPing       func() (time.Duration, error)
			closeSessions    func()
			openMu           sync.Mutex
		)

		if oursIsClient {
			ourCfg := ouryamux.DefaultConfig()
			ourCfg.AcceptBacklog = 256
			stockCfg := stockyamux.DefaultConfig()
			stockCfg.AcceptBacklog = 256
			c, s, cleanup := setupOursClientStockServer(t, ourCfg, stockCfg)
			clientOpenStream = func() (io.ReadWriteCloser, error) {
				return c.OpenStream()
			}
			serverAccept = func() (io.ReadWriteCloser, error) {
				return s.AcceptStream()
			}
			clientPing = c.Ping
			closeSessions = cleanup
		} else {
			stockCfg := stockyamux.DefaultConfig()
			stockCfg.AcceptBacklog = 256
			ourCfg := ouryamux.DefaultConfig()
			ourCfg.AcceptBacklog = 256
			c, s, cleanup := setupStockClientOursServer(t, stockCfg, ourCfg)
			clientOpenStream = func() (io.ReadWriteCloser, error) {
				openMu.Lock()
				stream, err := c.OpenStream()
				openMu.Unlock()
				return stream, err
			}
			serverAccept = func() (io.ReadWriteCloser, error) {
				return s.AcceptStream()
			}
			clientPing = c.Ping
			closeSessions = cleanup
		}

		atomic.AddUint64(totalCycles, 1)

		// Start server echo worker loop
		serverCtx, serverCancel := context.WithCancel(ctx)
		var serverWg sync.WaitGroup

		go func() {
			for {
				stream, err := serverAccept()
				if err != nil {
					return
				}
				serverWg.Add(1)
				go func(st io.ReadWriteCloser) {
					defer serverWg.Done()
					defer st.Close()
					buf := make([]byte, 32*1024)
					for {
						n, err := st.Read(buf)
						if n > 0 {
							if _, wErr := st.Write(buf[:n]); wErr != nil {
								return
							}
						}
						if err != nil {
							return
						}
					}
				}(stream)
			}
		}()

		// Run traffic on this session for either 15 seconds or until ctx cancelled
		sessionTimer := time.NewTimer(15 * time.Second)
		var clientWg sync.WaitGroup
		const concurrency = 4

		for worker := 0; worker < concurrency; worker++ {
			clientWg.Add(1)
			go func(wId int) {
				defer clientWg.Done()
				counter := 0
				for {
					select {
					case <-ctx.Done():
						return
					case <-serverCtx.Done():
						return
					default:
					}

					payload := payloads[counter%len(payloads)]
					counter++

					st, err := clientOpenStream()
					if err != nil {
						if ctx.Err() != nil || serverCtx.Err() != nil {
							return
						}
						t.Errorf("[%s] OpenStream error: %v", label, err)
						return
					}

					// Write data
					if _, err := st.Write(payload); err != nil {
						st.Close()
						if ctx.Err() != nil || serverCtx.Err() != nil {
							return
						}
						t.Errorf("[%s] Write error: %v", label, err)
						return
					}

					// Read echoed data
					recv := make([]byte, len(payload))
					if _, err := io.ReadFull(st, recv); err != nil {
						st.Close()
						if ctx.Err() != nil || serverCtx.Err() != nil {
							return
						}
						t.Errorf("[%s] ReadFull error: %v", label, err)
						return
					}

					if !bytes.Equal(recv, payload) {
						st.Close()
						t.Errorf("[%s] echoed payload mismatch (size %d)", label, len(payload))
						return
					}

					_ = st.Close()

					atomic.AddUint64(totalStreams, 1)
					atomic.AddUint64(totalBytes, uint64(len(payload)*2))

					// Periodic ping
					if counter%20 == 0 {
						if _, pErr := clientPing(); pErr == nil {
							atomic.AddUint64(totalPings, 1)
						}
					}
				}
			}(worker)
		}

		// Wait for session timer or ctx done
		select {
		case <-sessionTimer.C:
		case <-ctx.Done():
			sessionTimer.Stop()
		}

		serverCancel()
		clientWg.Wait()
		closeSessions()
		serverWg.Wait()
	}
}
