package interop

import (
	"crypto/rand"
	"crypto/sha256"
	"net"
	"testing"

	stockyamux "github.com/hashicorp/yamux"
	ouryamux "github.com/qiangli/yamux"
)

func randomPayload(n int) []byte {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return buf
}

func payloadHash(b []byte) [32]byte {
	return sha256.Sum256(b)
}

func setupOursClientStockServer(t *testing.T, ourCfg *ouryamux.Config, stockCfg *stockyamux.Config) (*ouryamux.Session, *stockyamux.Session, func()) {
	t.Helper()
	cConn, sConn := net.Pipe()

	client, err := ouryamux.Client(cConn, ourCfg)
	if err != nil {
		cConn.Close()
		sConn.Close()
		t.Fatalf("our Client init: %v", err)
	}

	server, err := stockyamux.Server(sConn, stockCfg)
	if err != nil {
		client.Close()
		sConn.Close()
		t.Fatalf("stock Server init: %v", err)
	}

	cleanup := func() {
		client.Close()
		server.Close()
		cConn.Close()
		sConn.Close()
	}
	return client, server, cleanup
}

func setupStockClientOursServer(t *testing.T, stockCfg *stockyamux.Config, ourCfg *ouryamux.Config) (*stockyamux.Session, *ouryamux.Session, func()) {
	t.Helper()
	cConn, sConn := net.Pipe()

	client, err := stockyamux.Client(cConn, stockCfg)
	if err != nil {
		cConn.Close()
		sConn.Close()
		t.Fatalf("stock Client init: %v", err)
	}

	server, err := ouryamux.Server(sConn, ourCfg)
	if err != nil {
		client.Close()
		sConn.Close()
		t.Fatalf("our Server init: %v", err)
	}

	cleanup := func() {
		client.Close()
		server.Close()
		cConn.Close()
		sConn.Close()
	}
	return client, server, cleanup
}
