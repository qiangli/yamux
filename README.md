# yamux

A permissively licensed (Apache-2.0 OR MIT) Go implementation of the yamux
stream multiplexer, wire-compatible with hashicorp/yamux and libp2p/go-yamux.

## Clean-room implementation

This is a **clean-room reimplementation**, based on two sources only:

1. **HashiCorp's protocol specification**,
   [`spec.md`](https://github.com/hashicorp/yamux/blob/master/spec.md). It is
   read for the protocol's facts (frame layout, types, flags, window semantics,
   ping, go-away, error codes) and is not copied or vendored here. Our own
   description of the protocol is in [`PROTOCOL.md`](PROTOCOL.md).
2. **The Rust implementation**,
   [libp2p/rust-yamux](https://github.com/libp2p/rust-yamux) (Apache-2.0 OR MIT),
   pinned at commit `6610cff7b3fe81aeb4fda9fa96d7ac4b3b2dbaf8`. Its design and
   logic are ported to idiomatic Go.

No source code from hashicorp/yamux or libp2p/go-yamux (both MPL-2.0) was read
or copied. Only their public Go API (exported names and signatures) is matched,
so this module works as a drop-in replacement. The stock MPL modules are used
only in a separate, test-only interop module (`interop/`) to prove wire
compatibility. They are never a dependency of this module.

It is built as a **drop-in replacement** for both MPL-2.0 Go implementations:

| Replaces | Package | Use |
|---|---|---|
| `github.com/hashicorp/yamux` | `github.com/qiangli/yamux` | `replace github.com/hashicorp/yamux => github.com/qiangli/yamux vX.Y.Z` |
| `github.com/libp2p/go-yamux/v5` | see the v5 compatibility package (planned) | `replace github.com/libp2p/go-yamux/v5 => ...` |

Status: **work in progress.** Do not use it in production until the first
tagged release passes the interop suite against stock hashicorp/yamux and
libp2p/go-yamux in both directions.

## Usage

Import it as a normal module, or use it as a drop-in for the MPL-2.0
implementations by adding a `replace` directive to your `go.mod`:

```
// Replace hashicorp/yamux (available now):
replace github.com/hashicorp/yamux => github.com/qiangli/yamux v0.1.0

// Replace libp2p/go-yamux/v5 (DEFERRED: the v5 compatibility package is
// planned and not yet available; do not use yet):
// replace github.com/libp2p/go-yamux/v5 => github.com/qiangli/yamux/v5 v0.1.0
```

Direct use:

```go
import "github.com/qiangli/yamux"

session, err := yamux.Client(conn, nil)
```

## License

Dual-licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option, the same as rust-yamux. See [NOTICE](NOTICE)
for provenance.
