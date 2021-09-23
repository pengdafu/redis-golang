package main

import (
	"log"
	"sync/atomic"
	"time"
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
		acceptCommonHandler(connCreateAcceptedSocket(cfd), 0, cip)
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
		log.Printf("Error registering fd event for the new client: %v (conn: todo)", GetLastErr(conn))
		connClose(conn)
		return
	}

	// todo client
	c.flags |= flags

	if err := connAccept(conn, clientAcceptHandler); err != nil {
		log.Printf("Error accepting a client connection: %v (conn: todo)", GetLastErr(conn))
		return
	}
}

func createClient(conn *Connection) *Client {
	c := new(Client)

	if conn != nil {
		_ = connNonBlock(conn)
		_ = connEnableTcpNoDelay(conn)
		if server.tcpKeepalive > 0 {
			_ = connKeepAlive(conn, server.tcpKeepalive)
		}
		connSetReadHandler(conn, readQueryFromClient)
		connSetPrivateData(conn, c)
	}

	_ = selectDb(c, 0)
	c.id = atomic.AddUint64(&server.nextClientId, 1)
	c.resp = 2
	c.conn = conn
	c.name = nil
	c.bufpos = 0
	c.qbPos = 0
	c.querybuf = sdsempty()
	c.pendingQueryBuf = sdsempty()
	c.querybufPeak = 0
	c.reqType = 0
	c.argc = 0
	c.argv = nil
	c.argvLenSum = 0
	c.originalArgc = 0
	c.originalArgv = nil
	c.cmd, c.lastCmd = nil, nil
	c.multiBulkLen = 0
	c.bulkLen = -1
	c.sentLen = 0
	c.flags = 0
	c.ctime = time.Duration(time.Now().Unix())
	c.lastInteraction = c.ctime
	clientSetDefaultAuth(c)
	c.replState = REPL_STATE_NONE
	c.replPutOnlineOnAck = 0
	c.replOff = 0
	c.readReplOff = 0
	c.replAckOff = 0
	c.replAckTime = 0
	c.slaveListeningPort = 0
	c.slaveAddr = ""
	c.slaveCapa = SLAVE_CAPA_NONE
	c.reply = listCreate()
	c.replyBytes = 0
	c.obufSoftLimitReachedTime = 0
	listSetFreeMethod(c.reply, freeClientReplyValue)
	listSetDupMethod(c.reply, dupClientReplyValue)
	c.bType = BLOCKED_NONE
	c.bpop.timeout = 0
	c.bpop.keys = dictCreate(nil, nil)
	c.bpop.target = nil
	c.bpop.xReadGroup = nil
	c.bpop.xReadConsumer = nil
	c.bpop.xReadGroupNoAck = 0
	c.bpop.numReplicas = 0
	c.bpop.replOffset = 0
	c.woff = 0
	c.watchedKeys = listCreate()
	c.pubSubChannels = dictCreate(nil, nil)
	c.pubSubPatterns = listCreate()
	c.peerId = nil
	c.sockName = nil
	c.clientListNode = nil
	c.pausedListNode = nil
	c.clientTrackingRedirection = 0
	c.clientTrackingPrefixes = nil
	c.clientCronLastMemoryUsage = 0
	c.clientCronLastMemoryType = CLIENT_TYPE_NORMAL
	c.authCallback = nil
	c.authCallbackPrivdata = nil
	c.authModule = nil
	listSetFreeMethod(c.pubSubPatterns, nil)
	listSetDupMethod(c.pubSubPatterns, nil)
	if conn != nil {
		linkClient(c)
	}
	initClientMultiState(c)
	return c
}

// readQueryFromClient 解析输入
func readQueryFromClient(conn *Connection) {

}

func clientSetDefaultAuth(c *Client) {
	c.user = DefaultUser
	c.authenticated = c.user.flags&USER_FLAG_NOPASS != 0 && !(c.user.flags&USER_FLAG_DISABLED != 0)
}

func freeClientReplyValue(o interface{}) {

}
func dupClientReplyValue(o interface{}) interface{} {
	return nil
}

func linkClient(c *Client) {

}

func clientAcceptHandler(conn *Connection) {
	c := connGetPrivateData(conn).(*Client)

	if connGetState(conn) != CONN_STATE_CONNECTED {
		log.Printf("Error accepting a client connection: %s", GetLastErr(conn))
		return
	}

	if server.protectedMode == 1 &&
		server.bindAddrCount == 0 &&
		DefaultUser.flags & USER_FLAG_NOPASS != 0 &&
		c.flags & CLIENT_UNIX_SOCKET == 0 {

	}
}
