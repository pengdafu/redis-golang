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
		Close:           nil,
		Accept:          nil,
		SetWriteHandler: nil,
		SetReadHandler:  nil,
		GetLastError:    nil,
		BlockingConnect: nil,
		SyncWrite:       nil,
		SyncRead:        nil,
		SyncReadline:    nil,
		GetType:         nil,
	}
}

func connSocketEventHandler(el *pkg.AeEventLoop, fd int, clientData interface{}, mask int) {

}
