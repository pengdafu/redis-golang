package main

import (
	"fmt"
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

type Connection struct {
	Type         *ConnectionType
	State        int
	Flags        int //short int
	Refs         int // short int
	LastErrNo    int
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
	Write           func(conn *Connection, data interface{}, dataLen int)
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

func ConnCreateAcceptedSocket(cfd int, CT_Socket *ConnectionType) *Connection {
	conn := connCreateSocket(CT_Socket)
	conn.Fd = cfd
	conn.State = CONN_STATE_ACCEPTING
	return conn
}

func connCreateSocket(CT_Socket *ConnectionType) *Connection {
	conn := new(Connection)
	conn.Fd = -1
	conn.Type = CT_Socket
	return conn
}

func (c *Connection) GetState() int {
	return c.State
}

func GetLastErr(conn *Connection) error {
	return fmt.Errorf("error: %d", conn.LastErrNo)
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
		Write:           nil,
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

	}
}
