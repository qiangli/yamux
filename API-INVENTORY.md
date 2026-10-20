# Yamux Public API Inventory

Signatures below are taken from `go doc` output for the two upstream modules (exported names and types only, no copied source). Clean-room basis is HashiCorp `spec.md` plus rust-yamux at `6610cff7b3fe81aeb4fda9fa96d7ac4b3b2dbaf8`.

## 1. `github.com/hashicorp/yamux` (root API)

Package `yamux` — `import "github.com/hashicorp/yamux"`. Observed at `v0.1.2` (frp pins `v0.1.1` with a local fork).

### 1.1 Config

```go
type Config struct {
    AcceptBacklog          int
    EnableKeepAlive        bool
    KeepAliveInterval      time.Duration
    ConnectionWriteTimeout time.Duration
    MaxStreamWindowSize    uint32
    StreamOpenTimeout      time.Duration
    StreamCloseTimeout     time.Duration
    LogOutput              io.Writer
    Logger                 Logger
}

func DefaultConfig() *Config
func (c *Config) Clone() *Config
func VerifyConfig(config *Config) error
```

`DefaultConfig` returns a config with sensible defaults (backlog, window sizes, keepalive disabled by default). `VerifyConfig` rejects mutually exclusive logger settings and illegal window/timeout values.

### 1.2 Logger and NetError

```go
type Logger interface {
    Print(v ...interface{})
    Printf(format string, v ...interface{})
    Println(v ...interface{})
}

type NetError struct { /* unexported */ }
func (e *NetError) Error() string
func (e *NetError) Temporary() bool
func (e *NetError) Timeout() bool
```

Either `LogOutput` or `Logger` may be set, not both.

### 1.3 Session creation

```go
func Client(conn io.ReadWriteCloser, config *Config) (*Session, error)
func Server(conn io.ReadWriteCloser, config *Config) (*Session, error)
```

`Client` and `Server` differ only in the parity of locally allocated stream identifiers (odd vs even).

### 1.4 Session methods

```go
type Session struct { /* unexported */ }

func (s *Session) Accept() (net.Conn, error)
func (s *Session) AcceptStream() (*Stream, error)
func (s *Session) AcceptStreamWithContext(ctx context.Context) (*Stream, error)
func (s *Session) Open() (net.Conn, error)
func (s *Session) OpenStream() (*Stream, error)
func (s *Session) Close() error
func (s *Session) GoAway() error
func (s *Session) Ping() (time.Duration, error)
func (s *Session) NumStreams() int
func (s *Session) IsClosed() bool
func (s *Session) CloseChan() <-chan struct{}
func (s *Session) Addr() net.Addr
func (s *Session) LocalAddr() net.Addr
func (s *Session) RemoteAddr() net.Addr
```

Notes from `go doc`:

* `Open` / `OpenStream` differ historically by return type (`net.Conn` vs `*Stream`); both open a new outbound stream.
* `Accept` / `AcceptStream` / `AcceptStreamWithContext` block until an inbound stream arrives or the session ends.
* `GoAway` initiates a graceful session shutdown (GoAway code 0).
* `Ping` sends a keepalive ping and waits for the pong, returning RTT.
* `NumStreams`, `IsClosed`, `CloseChan`, `Addr`/`LocalAddr`/`RemoteAddr` mirror `net.Conn` introspection.

### 1.5 Stream (net.Conn plus StreamID)

```go
type Stream struct { /* unexported */ }

func (s *Stream) Close() error
func (s *Stream) Read(b []byte) (n int, err error)
func (s *Stream) Write(b []byte) (n int, err error)
func (s *Stream) LocalAddr() net.Addr
func (s *Stream) RemoteAddr() net.Addr
func (s *Stream) Session() *Session
func (s *Stream) SetDeadline(t time.Time) error
func (s *Stream) SetReadDeadline(t time.Time) error
func (s *Stream) SetWriteDeadline(t time.Time) error
func (s *Stream) Shrink()
func (s *Stream) StreamID() uint32
```

`Stream` implements `net.Conn`; `StreamID` exposes the underlying yamux identifier. `Session()` returns the parent session. `Shrink` hints the read buffer may compact.

### 1.6 Exported error values

```go
var (
    ErrInvalidVersion          = fmt.Errorf("invalid protocol version")
    ErrInvalidMsgType          = fmt.Errorf("invalid msg type")
    ErrSessionShutdown         = fmt.Errorf("session shutdown")
    ErrStreamsExhausted        = fmt.Errorf("streams exhausted")
    ErrDuplicateStream         = fmt.Errorf("duplicate stream initiated")
    ErrRecvWindowExceeded      = fmt.Errorf("recv window exceeded")
    ErrTimeout                 = &NetError{/* i/o deadline reached, timeout: true */}
    ErrStreamClosed            = fmt.Errorf("stream closed")
    ErrUnexpectedFlag          = fmt.Errorf("unexpected flag")
    ErrRemoteGoAway            = fmt.Errorf("remote end is not accepting connections")
    ErrConnectionReset         = fmt.Errorf("connection reset")
    ErrConnectionWriteTimeout  = fmt.Errorf("connection write timeout")
    ErrKeepAliveTimeout        = fmt.Errorf("keepalive timeout")
)
```

These are the sentinels callers compare with `errors.Is` / direct equality depending on version.

---

## 2. Subset used by frp v0.68.1

frp imports `fmux "github.com/hashicorp/yamux"` and is pinned (via `replace`) to `github.com/fatedier/yamux v0.0.0-20250825093530-d0154be01cd6` (fork of `v0.1.1`). Grepped call sites in `frp@v0.68.1`:

* `client/connector.go` and `server/service.go`:
  * `fmux.DefaultConfig()`
  * `fmuxCfg.KeepAliveInterval = time.Duration(...Transport.TCPMuxKeepaliveInterval) * time.Second`
  * `fmuxCfg.LogOutput = xlog.NewTraceWriter(...)` (trace level)
  * `fmuxCfg.MaxStreamWindowSize = 6 * 1024 * 1024`
  * `fmux.Client(conn, fmuxCfg)` / `fmux.Server(frpConn, fmuxCfg)`
  * `muxSession.OpenStream()` and `session.AcceptStream()` (the `*Stream` variants)
  * `muxSession.Close()` on shutdown
* Server also multiplexes QUIC and yamux behind the same accept path; frp does not use `GoAway`, `Ping`, `VerifyConfig`, `Config.Logger`, `Config.ConnectionWriteTimeout`, `Config.StreamOpenTimeout/StreamCloseTimeout`, or stream deadlines — only the four config fields above and the open/accept/close trio. Any drop-in replacement must preserve at least those.

---

## 3. `github.com/libp2p/go-yamux/v5` API (used by `go-libp2p/p2p/muxer/yamux`)

Package `yamux` — `import "github.com/libp2p/go-yamux/v5"`. Observed at `v5.1.0` via `go doc`.

### 3.1 Config

```go
type Config struct {
    AcceptBacklog          int
    PingBacklog            int
    EnableKeepAlive        bool
    KeepAliveInterval      time.Duration
    MeasureRTTInterval     time.Duration
    ConnectionWriteTimeout time.Duration
    MaxIncomingStreams     uint32
    InitialStreamWindowSize uint32
    MaxStreamWindowSize    uint32
    LogOutput              io.Writer
    ReadBufSize            int
    WriteCoalesceDelay     time.Duration
    MaxMessageSize         uint32
}

func DefaultConfig() *Config
func VerifyConfig(config *Config) error
```

Differences from the root API: `PingBacklog`, `MeasureRTTInterval`, `MaxIncomingStreams`, `InitialStreamWindowSize`, `ReadBufSize`, `WriteCoalesceDelay`, `MaxMessageSize` are added; `StreamOpenTimeout`/`StreamCloseTimeout`/`Logger` are absent.

### 3.2 MemoryManager

```go
type MemoryManager interface {
    ReserveMemory(size int, prio uint8) error
    ReleaseMemory(size int)
    Done()
}
```

Passed as a factory to `Client`/`Server`; called when opening/accepting a stream (highest priority) and when growing the receive window (lower priority). Mirrors a subset of libp2p's `ResourceScopeSpan`.

### 3.3 Session

```go
type Session struct { /* unexported */ }

func Client(conn net.Conn, config *Config, mm func() (MemoryManager, error)) (*Session, error)
func Server(conn net.Conn, config *Config, mm func() (MemoryManager, error)) (*Session, error)

func (s *Session) Accept() (net.Conn, error)
func (s *Session) AcceptStream() (*Stream, error)
func (s *Session) Open(ctx context.Context) (net.Conn, error)
func (s *Session) OpenStream(ctx context.Context) (*Stream, error)
func (s *Session) Close() error
func (s *Session) CloseWithError(errCode uint32) error
func (s *Session) GoAway() error
func (s *Session) Ping() (dur time.Duration, err error)
func (s *Session) RTT() time.Duration
func (s *Session) NumStreams() int
func (s *Session) IsClosed() bool
func (s *Session) CloseChan() <-chan struct{}
func (s *Session) Addr() net.Addr
func (s *Session) LocalAddr() net.Addr
func (s *Session) RemoteAddr() net.Addr
```

Key differences: `Open`/`OpenStream` take a `context.Context`; `CloseWithError` exposes the GoAway code; `RTT()` is a non-blocking getter for the last measured RTT; `Client`/`Server` take a `net.Conn` (not just `io.ReadWriteCloser`) plus the optional `MemoryManager` factory.

### 3.4 Stream

```go
type Stream struct { /* unexported */ }

func (s *Stream) Close() error
func (s *Stream) CloseRead() error
func (s *Stream) CloseWrite() error
func (s *Stream) Reset() error
func (s *Stream) ResetWithError(errCode uint32) error
func (s *Stream) Read(b []byte) (n int, err error)
func (s *Stream) Write(b []byte) (int, error)
func (s *Stream) LocalAddr() net.Addr
func (s *Stream) RemoteAddr() net.Addr
func (s *Stream) Session() *Session
func (s *Stream) SetDeadline(t time.Time) error
func (s *Stream) SetReadDeadline(t time.Time) error
func (s *Stream) SetWriteDeadline(t time.Time) error
func (s *Stream) StreamID() uint32
```

v5 adds `CloseRead`/`CloseWrite` (half-close), `Reset`/`ResetWithError` (RST), and `ResetWithError` carries a uint32 application error code. `Shrink()` from the root API is absent.

### 3.5 Error types and sentinels

```go
type Error struct { /* unexported; msg string; timeout, temporary bool */ }
func (ye *Error) Error() string
func (ye *Error) Temporary() bool
func (ye *Error) Timeout() bool

type StreamError struct {
    ErrorCode uint32
    Remote    bool
}
func (s *StreamError) Error() string
func (s *StreamError) Is(target error) bool

type GoAwayError struct {
    ErrorCode uint32
    Remote    bool
}
func (e *GoAwayError) Error() string
func (e *GoAwayError) Is(target error) bool
func (e *GoAwayError) Temporary() bool
func (e *GoAwayError) Timeout() bool

type MemoryManager = interface{ ReserveMemory(int, uint8) error; ReleaseMemory(int); Done() }

var (
    ErrInvalidVersion         = &Error{msg: "invalid protocol version"}
    ErrInvalidMsgType         = &Error{msg: "invalid msg type"}
    ErrSessionShutdown        = &GoAwayError{ErrorCode: 0, Remote: false}
    ErrStreamsExhausted       = &Error{msg: "streams exhausted"}
    ErrDuplicateStream        = &Error{msg: "duplicate stream initiated"}
    ErrRecvWindowExceeded     = &Error{msg: "recv window exceeded"}
    ErrTimeout                = &Error{msg: "i/o deadline reached", timeout: true, temporary: true}
    ErrStreamClosed           = &Error{msg: "stream closed"}
    ErrUnexpectedFlag         = &Error{msg: "unexpected flag"}
    ErrRemoteGoAway           = &GoAwayError{Remote: true, ErrorCode: 0}
    ErrStreamReset            = &Error{msg: "stream reset"}
    ErrConnectionWriteTimeout = &Error{msg: "connection write timeout", timeout: true}
    ErrKeepAliveTimeout       = &Error{msg: "keepalive timeout", timeout: true}
)
```

In v5, `Error`, `GoAwayError`, `StreamError` replace the root module's plain `error` sentinels and `NetError`; `ErrStreamReset` is new, and `ErrConnectionReset` from the root module is not exported here (reset surfaces as `StreamError`/`ErrStreamReset`).

---

## 4. libp2p `p2p/muxer/yamux` expectations

`go-libp2p`'s yamux muxer wraps `go-yamux/v5` behind the libp2p `mux.MuxedConn` interface:

* Calls `yamux.Client` / `yamux.Server` with a `MemoryManager` derived from the libp2p resource manager.
* Uses `OpenStream(ctx)` / `AcceptStream()` for stream lifecycle, `Stream.Reset()` / `ResetWithError` on resource-limit aborts, and `Stream.CloseRead`/`CloseWrite` for half-close.
* Checks for `StreamError`, `GoAwayError`, and `ErrStreamReset` via `errors.As` / `errors.Is`.
* Propagates `RTT()` and `Ping()` into transport metrics where available.

A clean-room replacement must preserve the v5 `Client`/`Server`/`Session`/`Stream`/`MemoryManager`/`StreamError`/`GoAwayError`/`ErrStreamReset` surface with identical signatures to be usable as `replace github.com/libp2p/go-yamux/v5 => github.com/qiangli/yamux/...`.

## 5. Signature provenance

All signatures above were obtained via `go doc <pkg> <Symbol>` against the live module cache (`go-yamux/v5@v5.1.0`, `hashicorp/yamux@v0.1.2`). No source file from either upstream was opened; only exported names and types are reproduced here.

