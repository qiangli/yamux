package v5replace

import (
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// countingScope is a network.PeerScope that records what the muxer reserves,
// so a test can see the resource-manager plumbing actually being driven.
type countingScope struct {
	spans    atomic.Int64
	reserved atomic.Int64
	released atomic.Int64
	done     atomic.Int64
}

var _ network.PeerScope = (*countingScope)(nil)

func (s *countingScope) ReserveMemory(size int, prio uint8) error {
	s.reserved.Add(int64(size))
	return nil
}

func (s *countingScope) ReleaseMemory(size int) { s.released.Add(int64(size)) }

func (s *countingScope) Stat() network.ScopeStat {
	return network.ScopeStat{Memory: s.reserved.Load() - s.released.Load()}
}

func (s *countingScope) BeginSpan() (network.ResourceScopeSpan, error) {
	s.spans.Add(1)
	return (*countingSpan)(s), nil
}

func (s *countingScope) Peer() peer.ID { return "" }

// countingSpan is one span rooted at a countingScope.
type countingSpan countingScope

var _ network.ResourceScopeSpan = (*countingSpan)(nil)

func (s *countingSpan) ReserveMemory(size int, prio uint8) error {
	return (*countingScope)(s).ReserveMemory(size, prio)
}

func (s *countingSpan) ReleaseMemory(size int) { (*countingScope)(s).ReleaseMemory(size) }

func (s *countingSpan) Stat() network.ScopeStat { return (*countingScope)(s).Stat() }

func (s *countingSpan) BeginSpan() (network.ResourceScopeSpan, error) {
	return (*countingScope)(s).BeginSpan()
}

func (s *countingSpan) Done() { (*countingScope)(s).done.Add(1) }
