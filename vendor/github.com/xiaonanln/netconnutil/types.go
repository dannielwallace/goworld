package netconnutil

import "net"

type Flushable interface {
	Flush() error
}

type FlushableConn interface {
	net.Conn
	Flushable
}
