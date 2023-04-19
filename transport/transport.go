//go:build !windows
// +build !windows

package transport

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pkg/errors"
)

// Open opens the unix domain socket with the provided path and timeout,
// returning a TTransport.
func Open(sockPath string, timeout time.Duration) (*thrift.TSocket, error) {
	addr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		return nil, errors.Wrapf(err, "resolving socket path '%s'", sockPath)
	}

	trans := thrift.NewTSocketFromAddrTimeout(addr, timeout, timeout)
	if err := trans.Open(); err != nil {
		return nil, errors.Wrap(err, "opening socket transport")
	}

	return trans, nil
}

func OpenServer(listenPath string, timeout time.Duration) (*thrift.TServerSocket, error) {
	addr, err := net.ResolveUnixAddr("unix", listenPath)
	if err != nil {
		return nil, errors.Wrapf(err, "resolving addr (%s)", addr)
	}

	return thrift.NewTServerSocketFromAddrTimeout(addr, 0), nil
}

func waitForSocket(sockPath string, timeout time.Duration) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := os.Stat(sockPath); err == nil {
				return nil
			}
		}
	}
}
