package yamux

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	core "github.com/qiangli/yamux"
)

// MemoryManager accounts for the memory a session commits on a caller's
// behalf. A session reserves when a stream is opened or accepted, and releases
// everything through Done when that stream ends. It is the subset of libp2p's
// resource-manager span that a multiplexer needs.
type MemoryManager interface {
	ReserveMemory(size int, prio uint8) error
	ReleaseMemory(size int)
	Done()
}

// streamScopePriority is the reservation priority for a whole stream; opening
// or accepting one is the highest-priority allocation a session makes.
const streamScopePriority uint8 = 255

// Config tunes a session's limits and liveness checks. The zero value is not
// usable; start from DefaultConfig.
type Config struct {
	// AcceptBacklog bounds how many accepted-but-unclaimed streams queue up
	// before further inbound streams are reset.
	AcceptBacklog int

	// PingBacklog bounds how many ping acknowledgements may queue.
	PingBacklog int

	// EnableKeepAlive turns on periodic pings to detect a dead peer.
	EnableKeepAlive bool

	// KeepAliveInterval is the gap between keepalive pings.
	KeepAliveInterval time.Duration

	// MeasureRTTInterval is the gap between round-trip-time measurements.
	MeasureRTTInterval time.Duration

	// ConnectionWriteTimeout bounds a single write to the underlying
	// connection; exceeding it is treated as a dead transport.
	ConnectionWriteTimeout time.Duration

	// MaxIncomingStreams bounds concurrent inbound streams. Streams past the
	// limit are reset immediately.
	MaxIncomingStreams uint32

	// InitialStreamWindowSize is the receive credit a new stream starts with.
	InitialStreamWindowSize uint32

	// MaxStreamWindowSize caps how far a stream's send window may grow.
	MaxStreamWindowSize uint32

	// LogOutput receives session diagnostics.
	LogOutput io.Writer

	// ReadBufSize sizes the transport read buffer; 0 disables buffering.
	ReadBufSize int

	// WriteCoalesceDelay is how long a small frame may wait to be batched
	// with another before it is flushed.
	WriteCoalesceDelay time.Duration

	// MaxMessageSize caps a single data frame so one stream cannot hold the
	// connection for an unbounded stretch.
	MaxMessageSize uint32
}

// DefaultConfig returns a configuration matching the defaults of the v5 API it
// replaces, so a consumer that only mutates a field or two behaves the same.
func DefaultConfig() *Config {
	return &Config{
		AcceptBacklog:           256,
		PingBacklog:             32,
		EnableKeepAlive:         true,
		KeepAliveInterval:       30 * time.Second,
		MeasureRTTInterval:      30 * time.Second,
		ConnectionWriteTimeout:  10 * time.Second,
		MaxIncomingStreams:      1000,
		InitialStreamWindowSize: core.DefaultStreamWindow,
		MaxStreamWindowSize:     16 * 1024 * 1024,
		LogOutput:               os.Stderr,
		ReadBufSize:             4096,
		WriteCoalesceDelay:      100 * time.Microsecond,
		MaxMessageSize:          64 * 1024,
	}
}

// minMaxMessageSize is the smallest data frame cap that still makes progress.
const minMaxMessageSize = 1024

// VerifyConfig reports values that would make a session unusable.
func VerifyConfig(config *Config) error {
	if config == nil {
		return errors.New("yamux: nil config")
	}
	if config.AcceptBacklog <= 0 {
		return errors.New("yamux: AcceptBacklog must be positive")
	}
	if config.PingBacklog < 0 {
		return errors.New("yamux: PingBacklog must not be negative")
	}
	if config.MaxIncomingStreams == 0 {
		return errors.New("yamux: MaxIncomingStreams must be positive")
	}
	if config.EnableKeepAlive && config.KeepAliveInterval <= 0 {
		return errors.New("yamux: KeepAliveInterval must be positive when keepalive is enabled")
	}
	if config.MeasureRTTInterval < 0 || config.ConnectionWriteTimeout < 0 || config.WriteCoalesceDelay < 0 {
		return errors.New("yamux: durations must not be negative")
	}
	if config.ReadBufSize < 0 {
		return errors.New("yamux: ReadBufSize must not be negative")
	}
	if config.InitialStreamWindowSize < core.DefaultStreamWindow {
		return fmt.Errorf("yamux: InitialStreamWindowSize must be at least %d", core.DefaultStreamWindow)
	}
	if config.MaxStreamWindowSize < config.InitialStreamWindowSize {
		return errors.New("yamux: MaxStreamWindowSize must be at least InitialStreamWindowSize")
	}
	if config.MaxMessageSize < minMaxMessageSize {
		return fmt.Errorf("yamux: MaxMessageSize must be at least %d", minMaxMessageSize)
	}
	return nil
}

// clone guards the session against later mutation of the caller's Config,
// which go-libp2p does share across sessions.
func (c *Config) clone() Config { return *c }

// coreConfig projects the v5 configuration onto the core session config.
//
// PingBacklog, MeasureRTTInterval, ReadBufSize and MaxMessageSize have no
// counterpart: the core answers pings inline from its read loop, refreshes the
// round-trip time from each keepalive ping, reads frames straight off the
// transport, and already splits writes into interleaving-sized frames. They are
// validated and carried so a caller's configuration survives a round trip.
func (c *Config) coreConfig() *core.Config {
	cfg := core.DefaultConfig()
	cfg.AcceptBacklog = c.AcceptBacklog
	cfg.EnableKeepAlive = c.EnableKeepAlive
	cfg.KeepAliveInterval = c.KeepAliveInterval
	cfg.ConnectionWriteTimeout = c.ConnectionWriteTimeout
	cfg.WriteCoalesceDelay = c.WriteCoalesceDelay
	cfg.LogOutput = c.LogOutput
	cfg.Logger = nil

	// The core uses one timeout for both a ping's reply and a missed
	// keepalive; the v5 surface has no separate field for it.
	timeout := c.ConnectionWriteTimeout
	if timeout <= 0 {
		timeout = c.KeepAliveInterval
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cfg.KeepAliveTimeout = timeout

	// A stream open is a single queued frame, and a close waits for the
	// peer's FIN; both are bounded by the one write timeout v5 exposes.
	cfg.StreamOpenTimeout = c.ConnectionWriteTimeout
	cfg.StreamCloseTimeout = c.ConnectionWriteTimeout

	cfg.MaxIncomingStreams = clampToInt(c.MaxIncomingStreams)
	cfg.MaxStreamWindowSize = c.MaxStreamWindowSize
	if cfg.MaxStreamWindowSize < core.DefaultStreamWindow {
		cfg.MaxStreamWindowSize = core.DefaultStreamWindow
	}
	return cfg
}

// clampToInt converts a uint32 limit to the core's int limit without wrapping
// on a 32-bit platform.
func clampToInt(v uint32) int {
	if uint64(v) > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(v)
}
