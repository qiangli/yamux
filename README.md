# yamux

A permissively licensed (Apache-2.0 OR MIT) Go implementation of the
[yamux](https://github.com/hashicorp/yamux/blob/master/spec.md) stream
multiplexer. It is ported from [libp2p/rust-yamux](https://github.com/libp2p/rust-yamux)
and wire-compatible with hashicorp/yamux and libp2p/go-yamux.

It is built as a **drop-in replacement** for both MPL-2.0 Go implementations:

| Replaces | Package | Use |
|---|---|---|
| `github.com/hashicorp/yamux` | `github.com/qiangli/yamux` | `replace github.com/hashicorp/yamux => github.com/qiangli/yamux vX.Y.Z` |
| `github.com/libp2p/go-yamux/v5` | see the v5 compatibility package (planned) | `replace github.com/libp2p/go-yamux/v5 => ...` |

Status: **work in progress.** Do not use it in production until the first
tagged release passes the interop suite against stock hashicorp/yamux and
libp2p/go-yamux in both directions.

## License

Dual-licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option, the same as rust-yamux. See [NOTICE](NOTICE)
for provenance.
