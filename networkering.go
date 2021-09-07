package main

import (
	"log"
)

const (
	MAX_ACCEPTS_PER_CALL = 1000
)

func AcceptTcpHandler(el *AeEventLoop, fd int, privdata interface{}, mask int) {
	max := MAX_ACCEPTS_PER_CALL
	var cip string
	var port int
	for max > 0 {
		max--
		cfd, err := AnetAccept(fd, &cip, &port)
		if err != nil {
			log.Println("Accepting client connection:", err)
			return
		}
		_ = AnetCloexec(cfd)
		log.Printf("Accepting %s:%d ", cip, port)
		acceptCommonHandler(ConnCreateAcceptedSocket(cfd, CT_Socket), 0, cip)
	}
}

func acceptCommonHandler(conn *Connection, flags int, ip string) {
	if conn.GetState() != CONN_STATE_ACCEPTING {
		log.Printf("Accepted client connection in error state: %v", conn.ConnGetLastError())
		return
	}
}
