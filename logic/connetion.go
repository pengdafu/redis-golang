package logic

import (
	"github.com/pengdafu/redis-golang/pkg/ae"
	"github.com/pengdafu/redis-golang/pkg/net"
)

var CT_Socket *net.ConnectionType

func init() {
	CT_Socket = &net.ConnectionType{
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

func connSocketEventHandler(el *ae.AeEventLoop, fd int, clientData interface{}, mask int) {

}
