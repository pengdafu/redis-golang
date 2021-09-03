package logic

import (
	"github.com/pengdafu/redis-golang/pkg/ae"
	"github.com/pengdafu/redis-golang/pkg/net"
	"log"
)

const (
	MAX_ACCEPTS_PER_CALL = 1000
)

func AcceptTcpHandler(el *ae.AeEventLoop, fd int, privdate interface{}, mask int) {
	max := MAX_ACCEPTS_PER_CALL
	var cip string
	var port int
	for max > 0 {
		max--
		cfd, err := net.AnetAccept(fd, &cip, &port)
		if err != nil {
			log.Println("Accepting client connection:", err)
			return
		}
		_ = net.AnetCloexec(cfd)
		log.Printf("Accepting %s:%d ", cip, port)
		acceptCommonHandler(net.ConnCreateAcceptedSocket(cfd, CT_Socket), 0, cip)
	}
}

func acceptCommonHandler(conn *net.Connection, flags int, ip string) {

}
