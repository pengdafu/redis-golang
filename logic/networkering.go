package logic

import (
	"github.com/pengdafu/redis-golang/pkg"
	"log"
)

const (
	MAX_ACCEPTS_PER_CALL = 1000
)

func AcceptTcpHandler(el *pkg.AeEventLoop, fd int, privdata interface{}, mask int) {
	max := MAX_ACCEPTS_PER_CALL
	var cip string
	var port int
	for max > 0 {
		max--
		cfd, err := pkg.AnetAccept(fd, &cip, &port)
		if err != nil {
			log.Println("Accepting client connection:", err)
			return
		}
		_ = pkg.AnetCloexec(cfd)
		log.Printf("Accepting %s:%d ", cip, port)
		acceptCommonHandler(pkg.ConnCreateAcceptedSocket(cfd, CT_Socket), 0, cip)
	}
}

func acceptCommonHandler(conn *pkg.Connection, flags int, ip string) {
	if conn.GetState() != pkg.CONN_STATE_ACCEPTING {
		log.Printf("Accepted client connection in error state: %v", conn.ConnGetLastError())
		return
	}
}
