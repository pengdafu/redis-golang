package logic

import (
	"github.com/pengdafu/redis-golang/pkg"
)

var CT_Socket *pkg.ConnectionType

func init() {
	CT_Socket = &pkg.ConnectionType{
		AeHandle:        connSocketEventHandler,
		Connect:         nil,
		Write:           nil,
		Read:            nil,
		Close:           connSocketClose,
		Accept:          nil,
		SetWriteHandler: nil,
		SetReadHandler:  nil,
		GetLastError:    pkg.GetLastErr,
		BlockingConnect: nil,
		SyncWrite:       nil,
		SyncRead:        nil,
		SyncReadline:    nil,
		GetType:         nil,
	}
}

func connSocketEventHandler(el *pkg.AeEventLoop, fd int, clientData interface{}, mask int) {

}

func ConnSocketClose(conn *pkg.Connection) {
	if conn.Fd != -1 {

	}
}
