package main

import (
	"log"
)

const (
	MAX_ACCEPTS_PER_CALL = 1000
)

func acceptTcpHandler(el *AeEventLoop, fd int, privdata interface{}, mask int) {
	max := MAX_ACCEPTS_PER_CALL
	var cip string
	var port int
	for max > 0 {
		max--
		cfd, err := anetAccept(fd, &cip, &port)
		if err != nil {
			log.Println("Accepting client connection:", err)
			return
		}
		_ = anetCloexec(cfd)
		log.Printf("Accepting %s:%d ", cip, port)
		acceptCommonHandler(ConnCreateAcceptedSocket(cfd), 0, cip)
	}
}

func acceptCommonHandler(conn *Connection, flags int, ip string) {
	if conn.GetState() != CONN_STATE_ACCEPTING {
		log.Printf("Accepted client connection in error state: %v", conn.ConnGetLastError())
		return
	}

	if len(server.clients)+getClusterConnectionsCount() >= server.maxclients {
		var err string
		if server.clusterEnabled {
			err = "-ERR max number of clients + cluster connections reached\r\n"
		} else {
			err = "-ERR max number of clients reached\r\n"
		}
		if connWrite(conn, err) != nil {
			// noting todo
		}
		server.statRejectedConn++
		connClose(conn)
		return
	}

	c := createClient(conn)
	if c == nil {
		connClose(conn)
		return
	}

	// todo client

}

func createClient(conn *Connection) *Client {
	client := new(Client)

	if conn != nil {
		_ = connNonBlock(conn)
		_ = connEnableTcpNoDelay(conn)
		if server.tcpKeepalive > 0 {
			_ = connKeepAlive(conn, server.tcpKeepalive)
		}
		connSetReadhanler(conn, readQueryFromClient)
		connSetPrivateData(conn, client)
	}

	// todo SelectDb()

	return client
}

func readQueryFromClient(conn *Connection) {

}