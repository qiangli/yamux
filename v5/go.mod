module github.com/qiangli/yamux/v5

go 1.27

toolchain go1.27.2

// The core lives in the repository root as github.com/qiangli/yamux. The
// required version is the tag that carries the GoAwayWithCode / ResetError
// additions this package needs; the replace keeps the two in step when they are
// developed together and is ignored once this module is consumed as a
// dependency.
require github.com/qiangli/yamux v0.2.0

replace github.com/qiangli/yamux => ../
