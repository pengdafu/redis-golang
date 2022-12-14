package main

import (
	"github.com/pengdafu/redis-golang/ae"
	"github.com/pengdafu/redis-golang/anet"
	"github.com/pengdafu/redis-golang/util"
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
	AeHandler       func(el *ae.EventLoop, fd int, clientData interface{}, mask int)
	Connect         func(conn *Connection, addr string, port int, sourceAddr string, connectHandler ConnectionCallbackFunc)
	Write           func(conn *Connection, data string) error
	Read            func(conn *Connection, sdsBuf []byte, readLen int) (int, error)
	Close           func(conn *Connection)
	Accept          func(conn *Connection, acceptHandler ConnectionCallbackFunc) error
	SetWriteHandler func(conn *Connection, handler ConnectionCallbackFunc, barrier int) error
	SetReadHandler  func(conn *Connection, handler ConnectionCallbackFunc) error
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
		AeHandler:       connSocketEventHandler,
		Connect:         nil,
		Write:           connSocketWrite,
		Read:            connSocketRead,
		Close:           connSocketClose,
		Accept:          connSocketAccept,
		SetWriteHandler: connSocketSetWriteHandler,
		SetReadHandler:  connSocketSetReadHandler,
		GetLastError:    GetLastErr,
		BlockingConnect: nil,
		SyncWrite:       nil,
		SyncRead:        nil,
		SyncReadline:    nil,
		GetType:         nil,
	}
}

func connSocketEventHandler(el *ae.EventLoop, fd int, clientData interface{}, mask int) {
	conn := clientData.(*Connection)
	if conn.State == CONN_STATE_CONNECTING && mask&ae.Writeable != 0 && conn.ConnHandler != nil {
		// todo when connhandler is not nil, we should deal it.
	}

	// 通常情况下，先调用read，再调用write
	// 当 WriteBarrier 被设置，两者调用顺序反一下

	invert := conn.Flags&CONN_FLAG_WRITE_BARRIER > 0

	if !invert && mask&ae.Readable > 0 && conn.ReadHandler != nil {
		if err := callHandler(conn, conn.ReadHandler); err != nil {
			return
		}
	}

	if mask&ae.Writeable > 0 && conn.WriteHandler != nil {
		if err := callHandler(conn, conn.WriteHandler); err != nil {
			return
		}
	}

	if invert && mask&ae.Readable > 0 && conn.ReadHandler != nil {
		if err := callHandler(conn, conn.ReadHandler); err != nil {
			return
		}
	}
}

func connSocketClose(conn *Connection) {
	if conn.Fd != -1 {
		server.el.AeDeleteFileEvent(conn.Fd, ae.Readable|ae.Writeable)
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

func connRead(conn *Connection, sdsBuf []byte, readLen int) (int, error) {
	return conn.Type.Read(conn, sdsBuf, readLen)
}

func connNonBlock(conn *Connection) error {
	if conn.Fd == -1 {
		return C_ERR
	}
	return anet.NonBlock(conn.Fd)
}

func connEnableTcpNoDelay(conn *Connection) error {
	if conn.Fd == -1 {
		return C_ERR
	}
	return anet.EnableTcpNoDelay(conn.Fd)
}

func connSocketWrite(conn *Connection, data string) error {
	_, err := syscall.Write(conn.Fd, util.String2Bytes(data))
	if err != nil {
		conn.LastErr = err

		if conn.State == CONN_STATE_CONNECTED {
			conn.State = CONN_STATE_ERROR
		}
	}
	return err
}

func connSocketRead(conn *Connection, sdsBuf []byte, readLen int) (int, error) {
	nread, err := syscall.Read(conn.Fd, sdsBuf)
	if err != nil {
		if err == syscall.EAGAIN {
			conn.LastErr = err

			if conn.State == CONN_STATE_CONNECTED {
				conn.State = CONN_STATE_ERROR
			}
		}
	}

	if nread == 0 {
		conn.State = CONN_STATE_CLOSED
	}
	return nread, nil
}

func connKeepAlive(conn *Connection, interval int) error {
	if conn.Fd == -1 {
		return C_ERR
	}
	return anet.KeepAlive(conn.Fd, interval)
}

func connSetPrivateData(conn *Connection, data interface{}) {
	conn.PrivateData = data
}

func connGetPrivateData(conn *Connection) interface{} {
	return conn.PrivateData
}

// link connSocketSetReadHandler
func connSetReadHandler(conn *Connection, fn ConnectionCallbackFunc) {
	conn.Type.SetReadHandler(conn, fn)
}

func connSetWriteHandlerWithBarrier(conn *Connection, fn ConnectionCallbackFunc, barrier int) {
	conn.Type.SetWriteHandler(conn, fn, barrier)
}

func connPeerToString(conn *Connection, port *int, fd2strType int) string {
	return anet.FdToString(conn.Fd, port, fd2strType)
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

func connSocketSetWriteHandler(conn *Connection, fn ConnectionCallbackFunc, barrier int) error {
	if conn.WriteHandler != nil {
		return nil
	}

	conn.WriteHandler = fn
	if barrier > 0 {
		conn.Flags |= CONN_FLAG_WRITE_BARRIER
	} else {
		conn.Flags &= ^CONN_FLAG_WRITE_BARRIER
	}

	if conn.WriteHandler == nil {
		server.el.AeDeleteFileEvent(conn.Fd, ae.Writeable)
	} else {
		return server.el.AeCreateFileEvent(conn.Fd, ae.Writeable, conn.Type.AeHandler, conn)
	}
	return nil
}

func connSocketSetReadHandler(conn *Connection, fn ConnectionCallbackFunc) error {
	if conn.ReadHandler != nil {
		return nil
	}

	conn.ReadHandler = fn
	if conn.ReadHandler == nil {
		server.el.AeDeleteFileEvent(conn.Fd, ae.Readable)
	} else {
		return server.el.AeCreateFileEvent(conn.Fd, ae.Readable, conn.Type.AeHandler, conn)
	}
	return nil
}

func connGetState(conn *Connection) int {
	return conn.State
}
