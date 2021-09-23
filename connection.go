package main

import (
	"syscall"
	"time"

	"github.com/pengdafu/redis-golang/pkg"
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
	Accept          func(conn *Connection, acceptHandler ConnectionCallbackFunc) error
	SetWriteHandler func(conn *Connection, handler ConnectionCallbackFunc, barrier int)
	SetReadHandler  func(conn *Connection, handler ConnectionCallbackFunc)
	GetLastError    func(conn *Connection) error
	BlockingConnect func(conn *Connection, addr string, port int, timeout time.Duration)
	SyncWrite       func(conn *Connection, ptr string, size int, timeout time.Duration)
	SyncRead        func(conn *Connection, ptr string, size int, timeout time.Duration)
	SyncReadline    func(conn *Connection, ptr string, size int, timeout time.Duration)
	GetType         func(conn *Connection)
}

func connCreateAcceptedSocket(cfd int) *Connection {
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
		Accept:          connSocketAccept,
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
		server.el.aeDeleteFileEvent(conn.Fd, AE_READABLE|AE_WRITEABLE)
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

func connKeepAlive(conn *Connection, interval int) error {
	if conn.Fd == -1 {
		return C_ERR
	}
	return anetKeepAlive(conn.Fd, interval)
}

func connSetPrivateData(conn *Connection, data interface{}) {
	conn.PrivateData = data
}

func connGetPrivateData(conn *Connection) interface{} {
	return conn.PrivateData
}

func connSetReadHandler(conn *Connection, fc ConnectionCallbackFunc) {
	conn.Type.SetReadHandler(conn, fc)
}

func connPeerToString(conn *Connection, port *int) string {
	return anetFdToString()
}

func connAccept(conn *Connection, acceptHandler ConnectionCallbackFunc) error {
	return conn.Type.Accept(conn, acceptHandler)
}

func connIncrRefs(conn *Connection) {
	conn.Refs++
}
func connDecrRefs(conn *Connection) {
	conn.Refs--
}

// callHandler 用于连接去实现调用处理器
// 1. 增加refs保护这个连接
// 2. 执行处理器(如果设置)
// 3. 减少refs，并且如果refs==0，则关闭连接
func callHandler(conn *Connection, handler ConnectionCallbackFunc) error {
	connIncrRefs(conn)
	if handler != nil {
		handler(conn)
	}
	connDecrRefs(conn)
	if conn.Flags&CONN_FLAG_CLOSE_SCHEDULED != 0 {
		if !connHasRef(conn) {
			connClose(conn)
		}
		return C_ERR
	}
	return C_OK
}

func connSocketAccept(conn *Connection, acceptHandler ConnectionCallbackFunc) error {
	if conn.State != CONN_STATE_ACCEPTING {
		return C_ERR
	}

	conn.State = CONN_STATE_CONNECTED

	connIncrRefs(conn)
	if err := callHandler(conn, acceptHandler); err != nil {
		return err
	}
	connDecrRefs(conn)
	return C_OK
}

func connGetState(conn *Connection) int {
	return conn.State
}