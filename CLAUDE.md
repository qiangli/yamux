# CLAUDE.md

Go module `github.com/qiangli/yamux`: clean-room, Apache-2.0 OR MIT yamux
stream multiplexer, wire-compatible with hashicorp/yamux.

## Build and test

Always set `GOWORK=off`.

```sh
GOWORK=off go build ./...
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go test -run '^$' -fuzz '^FuzzDecodeHeader$' -fuzztime 60s .   # fuzz smoke
cd interop && GOWORK=off go test -race -skip TestSoak ./...                # interop matrix
```

`interop/` is a separate test-only module (own `go.mod`) that depends on stock
hashicorp/yamux. `TestSoak` is a long soak and is skipped in CI.

CI is `.github/workflows/ci.yml`: build, test, race, fuzz smoke, interop, and a
license check on the shipped module's dependency graph.

## Clean-room rules

- Never read or copy source from hashicorp/yamux or libp2p/go-yamux (MPL-2.0).
  Allowed sources: the protocol spec (`spec.md`) and rust-yamux (see README).
- The root module must have no third-party dependencies, and never depend on
  hashicorp/yamux or libp2p/go-yamux. Those live only in `interop/go.mod`.
- Shipped licenses stay Apache-2.0 OR MIT (plus BSD headers on any
  stdlib-derived file).
- No real hostnames in files. No GPU or cloud resources.
