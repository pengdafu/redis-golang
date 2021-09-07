package main

import (
	"github.com/pengdafu/redis-golang/pkg"
	"syscall"
	"time"
)

const (
	CONN_STATE_NONE = iota
	CONN_STATE_CONNECTING
	CONN_STATE_ACCEPTING
	CONN_STATE_CONNECTED
	CONN_STATE_CLOSED
	CONN_STATE_ERROR
)

const (
	CONN_FLAG_CLOSE_SCHEDULED = 1 << iota
	CONN_FLAG_WRITE_BARRIER
)

type Connection struct {
	Type         *ConnectionType
	State        int
	Flags        int //short int
	Refs         int // short int
	LastErr      error
	PrivateData  interface{}
	ConnHandler  ConnectionCallbackFunc
	WriteHandler ConnectionCallbackFunc
	ReadHandler  ConnectionCallbackFunc
	Fd           int
}

type ConnectionCallbackFunc func(conn *Connection)
type ConnectionType struct {
	AeHandle        func(el *AeEventLoop, fd int, clientData interface{}, mask int)
	Connect         func(conn *Connection, addr string, port int, sourceAddr string, connectHandler ConnectionCallbackFunc)
	Write           func(conn *Connection, data string) error
	Read            func(conn *Connection, buf interface{}, bufLen int)
	Close           func(conn *Connection)
	Accept          func(conn *Connection, acceptHandler ConnectionCallbackFunc)
	SetWriteHandler func(conn *Connection, handler ConnectionCallbackFunc, barrier int)
	SetReadHandler  func(conn *Connection, handler ConnectionCallbackFunc)
	GetLastError    func(conn *Connection) error
	BlockingConnect func(conn *Connection, addr string, port int, timeout time.Duration)
	SyncWrite       func(conn *Connection, ptr string, size int, timeout time.Duration)
	SyncRead        func(conn *Connection, ptr string, size int, timeout time.Duration)
	SyncReadline    func(conn *Connection, ptr string, size int, timeout time.Duration)
	GetType         func(conn *Connection)
}

func ConnCreateAcceptedSocket(cfd int) *Connection {
	conn := connCreateSocket()
	conn.Fd = cfd
	conn.State = CONN_STATE_ACCEPTING
	return conn
}

func connCreateSocket() *Connection {
	conn := new(Connection)
	conn.Fd = -1
	conn.Type = CT_Socket
	return conn
}

func (c *Connection) GetState() int {
	return c.State
}

func GetLastErr(conn *Connection) error {
	return conn.LastErr
}

func (c *Connection) ConnGetLastError() error {
	return c.Type.GetLastError(c)
}

func (c *Connection) Close() {
	c.Type.Close(c)
}

var CT_Socket *ConnectionType

func init() {
	CT_Socket = &ConnectionType{
		AeHandle:        connSocketEventHandler,
		Connect:         nil,
		Write:           connSocketWrite,
		Read:            nil,
		Close:           connSocketClose,
		Accept:          nil,
		SetWriteHandler: nil,
		SetReadHandler:  nil,
		GetLastError:    GetLastErr,
		BlockingConnect: nil,
		SyncWrite:       nil,
		SyncRead:        nil,
		SyncReadline:    nil,
		GetType:         nil,
	}
}

func connSocketEventHandler(el *AeEventLoop, fd int, clientData interface{}, mask int) {

}

func connSocketClose(conn *Connection) {
	if conn.Fd != -1 {
		server.el.AeDeleteFileEvent(conn.Fd, AE_READABLE|AE_WRITEABLE)
		syscall.Close(conn.Fd)
		conn.Fd = -1
	}

	if connHasRef(conn) {
		conn.Flags |= CONN_FLAG_CLOSE_SCHEDULED
		return
	}

	conn = nil
}

func connHasRef(conn *Connection) bool {
	return conn.Refs == 0
}

func connClose(conn *Connection) {
	conn.Type.Close(conn)
}

func connWrite(conn *Connection, data string) error {
	return conn.Type.Write(conn, data)
}

func connNonBlock(conn *Connection) error {
	if conn.Fd == -1 {
		return C_ERR
	}
	return anetNonBlock(conn.Fd)
}

func connEnableTcpNoDelay(conn *Connection) error {
	if conn.Fd == -1 {
		return C_ERR
	}
	return anetEnableTcpNoDelay(conn.Fd)
}

func connSocketWrite(conn *Connection, data string) error {
	_, err := syscall.Write(conn.Fd, pkg.String2Bytes(data))
	if err != nil {
		conn.LastErr = err

		if conn.State == CONN_STATE_CONNECTED {
			conn.State = CONN_STATE_ERROR
		}
	}
	return err
}
