# Architecture — Goroutine-Based Yamux over `net.Conn`

This document defines the planned concurrency layout for the clean-room Go port of rust-yamux (`6610cff7b3fe81aeb4fda9fa96d7ac4b3b2dbaf8`). It shows how Rust's `poll`-based state machine becomes Go goroutines and channels.

## 1. From poll to goroutines

The Rust crate drives progress through `Future`/`Stream` polling: a single `Connection` object is polled for new inbound streams, for the ability to create outbound streams, and for I/O readiness. Backpressure is expressed as `Poll::Pending` plus wakers. Go replaces that with blocking channels and dedicated goroutines:

* Every pending operation parks on a channel rather than returning `Pending`.
* Wakers become channel signals or timer expirations.
* The borrow-checked `parking_lot::Mutex` shared state becomes `sync.Mutex` / `sync.RWMutex` plus atomic counters.

The public shape stays compatible with hashicorp/yamux and libp2p/go-yamux/v5, but the interior is channel-driven.

## 2. Goroutines that exist

### 2.1 Session-level goroutines

| Goroutine | Count | Lifetime | Purpose |
|-----------|-------|----------|---------|
| `readLoop` | 1 | from `Client`/`Server` until `Close` or fatal error | Owns all reads from the underlying `net.Conn`. Decodes 12-byte headers, reassembles Data bodies, validates windows, dispatches per-stream payloads and session frames (Ping, WindowUpdate, GoAway). |
| `writeLoop` | 1 | same | Single writer to the underlying `net.Conn`. Serializes header encode, body writes, coalescing, and flush timing. No other goroutine writes to the conn directly. |
| `keepaliveLoop` / `rttLoop` | 0 or 1 | when `Config.EnableKeepAlive` | Periodically sends Ping (SYN), waits for pong (ACK), measures RTT, enforces liveness timeout. Implemented as a `time.Ticker`/`Timer` that posts a Ping frame to the write queue. |
| `acceptDispatch` | 0 or 1 (may be folded into `readLoop`) | same | Enqueues inbound streams into the `acceptCh` backlog. In the folded design, the `readLoop` does this inline; a separate goroutine is introduced only if decode and dispatch must be decoupled for fairness. |

No per-stream goroutines are created eagerly. A stream is a data holder with buffers and channels; its work happens on the caller's goroutine plus the two session loops.

### 2.2 Stream-level blocking

Streams block their callers on:

* `Read` — waits for bytes in the stream's `chunks` buffer plus a readiness channel.
* `Write` — waits for credit in its send window (flow-controlled), then enqueues a Data frame.
* `OpenStream` — waits for stream-id availability, window, and optional ack backlog.
* `AcceptStream` — waits on `acceptCh`.

No long-lived stream goroutine is required; deadlines and resets are enforced by closing channels and waking parked readers/writers.

## 3. Ownership of the write path

A single `writeLoop` owns the conn's `Write` side. All other paths send frames to it:

```
Stream.Write  ─┐
Open/SYN      ─┤
WindowUpdate  ─├─► writeQueue (chan Frame) ─► writeLoop ─► net.Conn.Write
Ping (keepalive)─┤                                    │
Close/FIN/RST ─┘                                     └─► WriteCoalesceDelay timer
                                                            (optional) flush
GoAway        ─┘
```

* `writeQueue` is a buffered channel sized to absorb bursts (one or two frames per stream). A fast `Stream.Write` never blocks on the syscall, only on flow-control or on the channel's backpressure.
* Headers are encoded with a single 12-byte `binary.BigEndian` write; Data bodies follow as a second write or are coalesced when `WriteCoalesceDelay` is set (libp2p v5).
* `ConnectionWriteTimeout` sets a write deadline on the underlying conn for each batch; exceeding it surfaces `ErrConnectionWriteTimeout` and tears the session down.
* `writeLoop` is the only place that calls `SetWriteDeadline` on the underlying conn; this avoids competing deadlines.

## 4. Read path and dispatch

`readLoop`:

1. Reads exactly 12 bytes for a header (handles short reads / `UnexpectedEOF`).
2. Validates version (must be 0) and type (0–3).
3. For `Data`, reads `Length` more bytes (capped by `MaxMessageSize` / `MIB` limit; oversize triggers `FrameTooLarge` and session termination).
4. For `Ping`, `WindowUpdate`, `GoAway`, acts on the header alone.
5. Looks up the stream by `StreamID` in a `map[uint32]*streamState` protected by a mutex:
   * If inbound SYN and the backlog (`AcceptBacklog`/`MaxIncomingStreams`) is full, immediately emits `RST` via `writeQueue`.
   * If window check fails (`ErrRecvWindowExceeded`), terminates the session.
   * Otherwise enqueues bytes into the stream's chunk buffer and signals its read channel; or enqueues a new stream into `acceptCh`.
   * Pong handling validates the echoed identifier and updates the RTT measurement.

## 5. Backpressure propagation

Backpressure appears at three levels:

* **Per-stream send window.** `Stream.Write` checks `sendWindow -= len(data)` under the stream's lock. If insufficient, it parks on a `windowCh` that the `readLoop` signals whenever a WindowUpdate arrives. Chunks are split at `DEFAULT_SPLIT_SEND_SIZE` / `MaxMessageSize` to interleave streams.
* **Receive window auto-tuning.** Each stream is initialized at `DEFAULT_CREDIT` (256 KiB) or `InitialStreamWindowSize` (v5). The receiver tracks consumed bytes; when half the window is consumed and consumption was fast relative to RTT, `maxReceiveWindow` is doubled up to `MaxStreamWindowSize` and the aggregate connection cap. Grants are sent as WindowUpdate deltas; no shrinking is performed.
* **Connection-level caps.** `MaxConnectionReceiveWindow` (Rust) / `MaxStreamWindowSize * MaxIncomingStreams` bound the sum of `maxReceiveWindow` across streams. Opening a new stream or growing a window may require reserving memory via `MemoryManager` (v5); failure resets the offending stream.
* **Stream-id and backlog limits.** `MAX_ACK_BACKLOG` (256) caps unacknowledged outbound opens; `AcceptBacklog` caps queued inbound streams; `MaxIncomingStreams` (v5) caps concurrent inbound. Exceeding any of them causes `TooManyStreams`-style errors or RST.
* **Write-queue pressure.** If `writeQueue` is full, `Stream.Write` blocks on enqueue rather than on the syscall. Priority: GoAway/RST/Ping ahead of Data if multiple queues are used; the simple design uses a single FIFO with bounded size.

## 6. Deadlines and how they close streams

Go's `net.Conn` deadline model is mapped onto stream-level timers:

* Each `streamState` holds `readTimer` and `writeTimer` (`*time.Timer`), plus `deadlineMu` and a `closed` flag.
* `SetReadDeadline(t)` / `SetWriteDeadline(t)` / `SetDeadline(t)` (which sets both) create or reset the corresponding timer. `t.IsZero()` stops the timer.
* On timer expiry, a goroutine (or the timer callback) closes a `readDone` / `writeDone` signal channel and optionally sends a `StreamError{Timeout:true}` wake. Any parked `Read`/`Write` unblocks with `ErrTimeout` (`*Error` with `timeout:true, temporary:true`) wrapping `os.ErrDeadlineExceeded`-style semantics to satisfy `net.Error`.
* `CloseRead` / `CloseWrite` (v5) enqueue a `FIN` and wake the opposite side's reader with `io.EOF`.
* `Reset()` / `ResetWithError(code)` enqueues a `RST` and closes both signal channels with a `StreamError` whose `ErrorCode` is the wire code.
* `Close()` on the stream performs a graceful half-close: sends `FIN`, waits up to `StreamCloseTimeout` for the peer's `FIN`/`RST`, then forces a reset if still half-open.

Session-level deadlines are similar: `ConnectionWriteTimeout` applies a write deadline to the underlying conn during `writeLoop` batches; `KeepAliveTimeout` fires if a Ping ACK is not seen within the interval and closes the session with `ErrKeepAliveTimeout`.

## 7. Shutdown, GoAway, and cleanup

* `Session.Close()` initiates a graceful shutdown: stops accepting new streams, drains `writeQueue`, sends a GoAway (code 0), optionally waits for peer FINs, then closes the underlying conn.
* `Session.CloseWithError(code)` (v5) sends GoAway with the given code.
* `Session.GoAway()` sends GoAway without closing the conn, preventing new opens while existing streams drain.
* On fatal errors (protocol violation, I/O error, window overflow), the session transitions to a cleanup path: all stream channels are closed, `CloseChan` is signaled, and the underlying conn is closed. This mirrors `connection/cleanup.rs` and `connection/closing.rs` which close receivers, drain pending frames, and flush a `Term` frame.
* `IsClosed()` and `CloseChan()` mirror the Go APIs; `CloseChan` is closed once and reused.

## 8. Buffer management

* Inbound bytes for a stream accumulate in a `Chunks` deque (mirroring `chunks.rs`), which presents a `bytes.Reader` view across multiple `Vec<u8>` payloads merged from successive Data frames. `Read` advances a cursor without copying when possible.
* A `ReadBufSize` (v5) may pre-allocate per-stream buffers; if zero, buffering is minimal.
* `Shrink()` (root API) hints that an idle stream may release its buffer.
* Outbound buffering is limited to `writeQueue` and per-stream split chunks; no extra copy beyond the `Write` caller buffer plus the frame header.

## 9. Rust-module to Go-file map

Every module from rust-yamux `yamux` crate at `6610cff7b3fe81aeb4fda9fa96d7ac4b3b2dbaf8` lands in a planned Go file (relative to repo root). Test-harness and `quickcheck-ext` crates are test-only shims and are not ported as production code.

| Rust module | Rust path | Planned Go file | Responsibility |
|-------------|-----------|-----------------|----------------|
| `yamux` (crate root) | `yamux/src/lib.rs` | `config.go` (+ `yamux.go` package doc) | `Config`, `DEFAULT_CREDIT`, `DEFAULT_SPLIT_SEND_SIZE`, `MAX_ACK_BACKLOG`, `Client`/`Server` constructors |
| `error` | `yamux/src/error.rs` | `errors.go` | `ConnectionError`, `Error`/`GoAwayError`/`StreamError`, exported sentinels `ErrInvalidVersion` etc. |
| `chunks` | `yamux/src/chunks.rs` | `chunks.go` | `Chunks`/`Chunk` deque used by `Stream` read path |
| `tagged_stream` | `yamux/src/tagged_stream.rs` | `internal/tagged_stream.go` | Helper to tag multiplexed stream-channel selects (`SelectAll<TaggedStream>` equivalent) |
| `frame` | `yamux/src/frame.rs` | `frame.go` | `Frame` generic wrapper, `data`/`windowUpdate`/`ping`/`term` constructors |
| `frame::header` | `yamux/src/frame/header.rs` | `header.go` | `Header`, `Version`/`Tag`/`Flags`/`StreamId`/`Len`, constants `SYN`/`ACK`/`FIN`/`RST`, `HEADER_SIZE`, `encode`/`decode` |
| `frame::io` | `yamux/src/frame/io.rs` | `frame_io.go` | `Io` reader/writer: 12-byte header read, body reassembly, `MAX_FRAME_BODY_LEN` guard, encode-then-write sequencing |
| `connection` | `yamux/src/connection.rs` | `session.go` | `Connection`/`Session`, `Mode` (client/server), `Id`, state machine (Active/Closing/Cleanup/Closed), dispatch, multiplex logic |
| `connection::stream` | `yamux/src/connection/stream.rs` | `stream.go` | `Stream`, `State`/`Flag`, `Shared` state, `Read`/`Write`/`Close`/`CloseRead`/`CloseWrite`/`Reset`/`SetDeadline` etc. |
| `connection::stream::flow_control` | `yamux/src/connection/stream/flow_control.rs` | `flow_control.go` | `FlowController`, window update logic, BDP auto-tuning, `accumulated_max_stream_windows` accounting |
| `connection::rtt` | `yamux/src/connection/rtt.rs` | `rtt.go` | `Rtt`/`RttState`, `nextPing`/`handlePong`, `PING_INTERVAL`, `RTT()` tracking |
| `connection::cleanup` | `yamux/src/connection/cleanup.rs` | `session_cleanup.go` | `Cleanup` future equivalent: close stream receivers, drain channels, surface session error |
| `connection::closing` | `yamux/src/connection/closing.rs` | `session_closing.go` | `Closing` future equivalent: drain receivers, flush pending frames, write `Term`, close socket |

Additional planned Go files with no direct single-module Rust counterpart (they factor cross-cutting concerns):

* `keepalive.go` — `keepaliveLoop` interval logic, coalesced with `rtt.go` if small (mirrors `Config::EnableKeepAlive`/`KeepAliveInterval`).
* `session_accept.go` (optional) — accept backlog enforcement if split from `session.go`.
* `netaddr.go` — `Addr`/`LocalAddr`/`RemoteAddr` wrappers around the underlying `net.Conn`.

Non-ported crates: `test-harness` and `quickcheck-ext` are test tooling (`benches/concurrent`, `tests/ack_*`, `poll_api`); their coverage is absorbed by the top-level Go tests and the `interop/` wire-compatibility harness.

## 10. Concurrency invariants

* Exactly one goroutine reads, exactly one writes. Mutexes protect the stream map and per-stream window/buffer; channels serialize stream open/accept.
* Closing `CloseChan` happens at most once via `sync.Once` or atomic.
* Stream reads never hold the global session lock while blocking; only short critical sections touch shared state.
* All timers are stopped and drained on stream/session close to avoid goroutine leaks.

