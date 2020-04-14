package netconnutil

import (
	"github.com/pkg/errors"
	"net"
	"runtime"
)

type noTempErrorConn struct {
	net.Conn
}

type temperaryError interface {
	Temporary() bool
}

func isTemporary(err error) bool {
	if err == nil {
		return false
	}

	err = errors.Cause(err)
	ne, ok := err.(temperaryError)
	return ok && ne.Temporary()
}

func (ntec noTempErrorConn) Read(b []byte) (n int, err error) {
	for {
		n, err = ntec.Conn.Read(b)
		if isTemporary(err) {
			if n > 0 {
				return n, nil
			} else {
				runtime.Gosched()
				continue
			}
		} else {
			return n, err
		}
	}
}

func (ntec noTempErrorConn) Write(b []byte) (n int, err error) {
	for {
		n, err = ntec.Conn.Write(b)
		if isTemporary(err) {
			if n > 0 {
				return n, nil
			} else {
				runtime.Gosched()
				continue
			}
		} else {
			return n, err
		}
	}
}

func NewNoTempErrorConn(conn net.Conn) net.Conn {
	return noTempErrorConn{
		Conn: conn,
	}
}
