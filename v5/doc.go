// Package yamux multiplexes streams over a single ordered connection and is a
// drop-in replacement for github.com/libp2p/go-yamux/v5, so a consumer can
// swap the MPL-2.0 implementation out with
//
//	replace github.com/libp2p/go-yamux/v5 => github.com/qiangli/yamux/v5 v5.0.0
//
// It is a thin, permissively licensed adapter over the clean-room core in
// github.com/qiangli/yamux: the core owns the wire protocol, this package owns
// the v5-shaped API — context-aware stream opening, the MemoryManager hook, and
// the Error/StreamError/GoAwayError surface.
//
// The package is wire-compatible with hashicorp/yamux and libp2p/go-yamux/v5,
// including the v5 convention of carrying a stream error code in the length
// field of a reset frame and a session error code in the length field of a
// go-away frame.
//
// Clean-room basis: the yamux protocol specification, libp2p/rust-yamux
// (Apache-2.0 OR MIT), the exported names and types of go-yamux/v5 as reported
// by `go doc`, and black-box observation of its wire output. No MPL-2.0 source
// was read or copied.
package yamux
