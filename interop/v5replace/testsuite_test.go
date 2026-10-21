package v5replace

import (
	"testing"

	tmux "github.com/libp2p/go-libp2p/p2p/muxer/testsuite"
	libp2pyamux "github.com/libp2p/go-libp2p/p2p/muxer/yamux"
)

// TestLibp2pMuxerTestsuite runs go-libp2p's own multiplexer conformance suite
// against its yamux muxer, which the replace directive in this module's go.mod
// has built on github.com/qiangli/yamux/v5. It is the acceptance gate for the
// drop-in: the suite covers concurrent stream opens, stream reset, write after
// close, streams left open, and several throughput stress shapes.
func TestLibp2pMuxerTestsuite(t *testing.T) {
	tmux.SubtestAll(t, libp2pyamux.DefaultTransport)
}
