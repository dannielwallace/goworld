package netconnutil

import (
	"github.com/golang/snappy"
	"net"
)

type snappyConn struct {
	net.Conn
	snappyWriter *snappy.Writer
	snappyReader *snappy.Reader
}

func (sc *snappyConn) Read(b []byte) (n int, err error) {
	return sc.snappyReader.Read(b)
}

func (sc *snappyConn) Write(b []byte) (n int, err error) {
	return sc.snappyWriter.Write(b)
}

func (sc *snappyConn) Flush() error {
	if err := sc.snappyWriter.Flush(); err != nil {
		return err
	}

	if f, ok := sc.Conn.(Flushable); ok {
		return f.Flush()
	}

	return nil
}

func NewSnappyConn(conn net.Conn) FlushableConn {
	return &snappyConn{
		Conn:         conn,
		snappyWriter: snappy.NewWriter(conn),
		snappyReader: snappy.NewReader(conn),
	}
}
