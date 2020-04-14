package netconnutil

import (
	"bufio"
	"net"
)

// NewBufferedConn creates a new connection with buffered write based on underlying connection
func NewBufferedConn(conn net.Conn, readBufferSize, writeBufferSize int) FlushableConn {
	bc := &bufferedConn{
		Conn:      conn,
		bufReader: bufio.NewReaderSize(conn, readBufferSize),
		bufWriter: bufio.NewWriterSize(conn, writeBufferSize),
	}
	return bc
}

// bufferedConn provides buffered write to connections
type bufferedConn struct {
	net.Conn
	bufReader *bufio.Reader
	bufWriter *bufio.Writer
}

// Read
func (bc *bufferedConn) Read(p []byte) (int, error) {
	return bc.bufReader.Read(p)
}

func (bc *bufferedConn) Write(p []byte) (int, error) {
	return bc.bufWriter.Write(p)
}

func (bc *bufferedConn) Close() error {
	bc.Flush()
	return bc.Conn.Close()
}

func (bc *bufferedConn) Flush() error {
	err := bc.bufWriter.Flush()
	if err != nil {
		return err
	}

	if f, ok := bc.Conn.(Flushable); ok {
		return f.Flush()
	}

	return nil
}
