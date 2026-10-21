module github.com/qiangli/yamux/interop

go 1.27

replace github.com/qiangli/yamux => ../

require (
	github.com/hashicorp/yamux v0.1.2
	github.com/libp2p/go-yamux/v5 v5.1.0
	github.com/qiangli/yamux v0.2.0
	github.com/qiangli/yamux/v5 v5.0.0
)

require github.com/libp2p/go-buffer-pool v0.0.2 // indirect

replace github.com/qiangli/yamux/v5 => ../v5
