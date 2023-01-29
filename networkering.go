package main

import (
	"bytes"
	"fmt"
	"github.com/pengdafu/redis-golang/adlist"
	"github.com/pengdafu/redis-golang/ae"
	"github.com/pengdafu/redis-golang/anet"
	"github.com/pengdafu/redis-golang/dict"
	"github.com/pengdafu/redis-golang/sds"
	"github.com/pengdafu/redis-golang/util"
	"log"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	MAX_ACCEPTS_PER_CALL = 1000
)

func acceptTcpHandler(el *ae.EventLoop, fd int, privdata interface{}, mask int) {
	max := MAX_ACCEPTS_PER_CALL
	var cip string
	var port int
	for max > 0 {
		max--
		cfd, err := anet.Accept(fd, &cip, &port)
		if err != nil {
			if err == syscall.EAGAIN {
				return
			}
			log.Println("Accepting client connection:", err)
			return
		}
		_ = anet.Cloexec(cfd)
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
		if connWrite(conn, err) <= 0 {
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
		freeClientAsync(c)
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
	c.querybuf = sds.Empty()
	c.pendingQueryBuf = sds.Empty()
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
	c.ctime = time.Now().Unix()
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
	c.reply = adlist.Create()
	c.replyBytes = 0
	c.obufSoftLimitReachedTime = 0
	c.reply.SetFreeMethod(freeClientReplyValue)
	c.reply.SetDupMethod(dupClientReplyValue)
	c.bType = BLOCKED_NONE
	c.bpop.timeout = 0
	c.bpop.keys = dict.Create(nil, nil)
	c.bpop.target = nil
	c.bpop.xReadGroup = nil
	c.bpop.xReadConsumer = nil
	c.bpop.xReadGroupNoAck = 0
	c.bpop.numReplicas = 0
	c.bpop.replOffset = 0
	c.woff = 0
	c.watchedKeys = adlist.Create()
	c.pubSubChannels = dict.Create(nil, nil)
	c.pubSubPatterns = adlist.Create()
	c.peerId = sds.Empty()
	c.sockName = sds.Empty()
	c.clientListNode = nil
	c.pausedListNode = nil
	c.clientTrackingRedirection = 0
	c.clientTrackingPrefixes = nil
	c.clientCronLastMemoryUsage = 0
	c.clientCronLastMemoryType = CLIENT_TYPE_NORMAL
	c.authCallback = nil
	c.authCallbackPrivdata = nil
	c.authModule = nil
	c.pubSubPatterns.SetFreeMethod(nil)
	c.pubSubPatterns.SetDupMethod(nil)
	if conn != nil {
		linkClient(c)
	}
	initClientMultiState(c)
	return c
}

// readQueryFromClient 解析输入
func readQueryFromClient(conn *Connection) {
	c := connGetPrivateData(conn).(*Client)
	var readLen, qblen int

	if postponeClientRead(c) {
		return
	}

	server.statTotalReadsProcessed++

	readLen = PROTO_REPLY_CHUNK_BYTES

	if c.reqType == PROTO_REQ_MULTIBULK && c.multiBulkLen > 0 && c.bulkLen != -1 &&
		c.bulkLen >= PROTO_MBULK_BIG_ARG {
		remaining := (c.bulkLen + 2) - sds.Len(c.querybuf)
		if remaining > 0 && remaining < readLen {
			readLen = remaining
		}
	}

	qblen = sds.Len(c.querybuf)
	if c.querybufPeak < qblen {
		c.querybufPeak = qblen
	}

	c.querybuf = sds.MakeRoomFor(c.querybuf, readLen)
	nread, err := connRead(c.conn, c.querybuf.Buf(qblen), readLen)
	if nread < 0 {
		return
	}
	if err != nil {
		if connGetState(conn) == CONN_STATE_CONNECTED {
			return
		} else {
			log.Printf("Reading from client: %v\n", conn.LastErr)
			freeClientAsync(c)
			return
		}
	} else if nread == 0 {
		log.Println("Client closed connection")
		freeClientAsync(c)
		return
	} else if c.flags&CLIENT_MASTER > 0 {
		// todo master pengding query buf
	}

	sds.IncrLen(c.querybuf, nread)
	c.lastInteraction = server.unixtime
	if c.flags&CLIENT_MASTER > 0 {
		c.readReplOff += nread
	}
	if sds.Len(c.querybuf) > server.clientMaxQueryBufLen {
		// todo overflow clientMaxQueryBufLen
		connWrite(conn, fmt.Sprintf("query buf len overflow: %d, curLen: %d", server.clientMaxQueryBufLen, sds.Len(c.querybuf)))
		freeClientAsync(c)
		return
	}

	processInputBuffer(c)
}

func processInputBuffer(c *Client) {
	// start parse
	for c.qbPos < sds.Len(c.querybuf) {
		// 客户端暂停了
		if c.flags&CLIENT_SLAVE == 0 && c.flags&CLIENT_PENDING_READ == 0 && clientsArePaused() {
			break
		}

		// 如果客户端正在进行其它操作，终止
		if c.flags&CLIENT_BLOCKED > 0 {
			break
		}

		// 如果已经有待处理的命令，则暂时不解析
		if c.flags&CLIENT_PENDING_COMMAND > 0 {
			break
		}

		if server.luaTimeout > 0 && c.flags&CLIENT_MASTER > 0 {
			break
		}

		if c.flags&(CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP) > 0 {
			break
		}

		if c.reqType == 0 {
			if c.querybuf.BufData(c.qbPos)[0] == '*' {
				c.reqType = PROTO_REQ_MULTIBULK
			} else {
				c.reqType = PROTO_REQ_INLINE
			}
		}

		if c.reqType == PROTO_REQ_INLINE {
			if processInlineBuffer(c) != C_OK {
				break
			}

			// todo gopher mode?
		} else if c.reqType == PROTO_REQ_MULTIBULK {
			if processMultibulkBuffer(c) != C_OK {
				break
			}
		} else {
			panic("Unknown request type")
		}

		if c.argc == 0 {
			resetClient(c)
		} else {
			// 如果处于IO线程的上下文中，不处理命令
			if c.flags&CLIENT_PENDING_READ > 0 {
				c.flags |= CLIENT_PENDING_COMMAND
				break
			}

			if processCommandAndResetClient(c) == C_ERR {
				return
			}
		}
	}
	if c.qbPos > 0 {
		sds.Range(c.querybuf, c.qbPos, -1)
		c.qbPos = 0
	}
}

func processInlineBuffer(c *Client) error {
	newLineIdx := bytes.IndexByte(c.querybuf.BufData(c.qbPos), '\n')
	lineFeedChars := 1
	if newLineIdx == -1 {
		if sds.Len(c.querybuf)-c.qbPos > PROTO_INLINE_MAX_SIZE {
			addReplyError(c, "Protocol error: too big inline request")
			setProtocolError("too big inline request", c)
		}
		return C_ERR
	}

	newline := c.querybuf.BufData(c.qbPos)[:newLineIdx+1]
	if len(newline) > 0 && c.querybuf.BufData(c.qbPos)[0] != newline[0] && newline[len(newline)-1] == '\r' {
		newline = newline[:len(newline)-1]
		lineFeedChars++
	}

	queryLen := len(newline)
	aux := sds.NewLen(util.Bytes2String(newline))
	argv, argc := sds.SplitArgs(aux)
	if len(argv) == 0 {
		addReplyError(c, "Protocol error: unbalanced quotes in request")
		setProtocolError("unbalanced quotes in inline request", c)
		return C_ERR
	}

	if queryLen == 0 && getClientType(c) == CLIENT_TYPE_SLAVE {
		c.replAckTime = server.unixtime
	}

	if queryLen != 0 && c.flags&CLIENT_MASTER > 0 {
		//sdsFreeSlipRes()
		log.Println("WARNING: Receiving inline protocol from master, master stream corruption? Closing the master connection and discarding the cached master.")
		setProtocolError("Master using the inline protocol. Desync?", c)
		return C_ERR
	}

	c.qbPos += queryLen /* + lineFeedChars */
	if argc > 0 {
		//if (c->argv) zfree(c->argv);
		//c->argv = zmalloc(sizeof(robj*)*argc);
		//c->argv_len_sum = 0;
		c.argvLenSum = 0
		c.argv = make([]*robj, argc)
		c.argc = 0
	}
	for i := 0; i < argc; i++ {
		c.argv[c.argc] = createObject(ObjString, argv[i])
		c.argc++
		c.argvLenSum += sds.Len(argv[i])
	}

	return C_OK
}

func processMultibulkBuffer(c *Client) error {
	if c.multiBulkLen == 0 {
		newLineIdx := bytes.IndexByte(c.querybuf.BufData(c.qbPos), '\r')
		if newLineIdx == -1 {
			if sds.Len(c.querybuf) > PROTO_INLINE_MAX_SIZE {
				addReplyError(c, "Protocol error: too big mbulk count string")
				setProtocolError("too big mbulk count string", c)
			}
			return C_ERR
		}

		/* Buffer should also contain \n */
		//if (newline-(c->querybuf+c->qb_pos) > (ssize_t)(sdslen(c->querybuf)-c->qb_pos-2))
		//return C_ERR;
		ll, err := strconv.ParseInt(util.Bytes2String(c.querybuf.BufData(c.qbPos)[1:newLineIdx]), 10, 64)
		if err != nil || ll > 1024*1024 {
			addReplyError(c, fmt.Sprintf("Protocol error: invalid multibulk length: %v, len: %d", err, ll))
			setProtocolError("invalid mbulk count", c)
			return C_ERR
		} else if ll > 10 && authRequired(c) {
			addReplyError(c, "Protocol error: unauthenticated multibulk length")
			setProtocolError("unauth mbulk count", c)
			return C_ERR
		}

		c.qbPos += newLineIdx + 2
		if ll <= 0 {
			return C_OK
		}
		c.multiBulkLen = int(ll)
		c.argv = make([]*robj, ll)
		c.argvLenSum = 0
	}

	for c.multiBulkLen > 0 {
		if c.bulkLen == -1 {
			newLineIdx := bytes.IndexByte(c.querybuf.BufData(c.qbPos), '\r')
			if newLineIdx == -1 {
				if sds.Len(c.querybuf) > PROTO_INLINE_MAX_SIZE {
					addReplyError(c, "Protocol error: too big bulk count string")
					setProtocolError("too big bulk count string", c)
					return C_ERR
				}
				break
			}

			if c.querybuf.BufData(c.qbPos)[0] != '$' {
				addReplyErrorFormat(c,
					"Protocol error: expected '$', got '%c'",
					c.querybuf.BufData(c.qbPos)[0])
				setProtocolError("expected $ but got something else", c)
				return C_ERR
			}

			ll, err := strconv.ParseInt(util.Bytes2String(c.querybuf.BufData(c.qbPos)[1:newLineIdx]), 10, 64)
			if err != nil || ll < 0 || (c.flags&CLIENT_MASTER == 0 &&
				ll > server.protoMaxBulkLen) {
				addReplyError(c, "Protocol error: invalid bulk length")
				setProtocolError("invalid bulk length", c)
				return C_ERR
			} else if ll > 16384 && authRequired(c) {
				addReplyError(c, "Protocol error: unauthenticated bulk length")
				setProtocolError("unauth bulk length", c)
				return C_ERR
			}

			c.qbPos += newLineIdx + 2
			if ll >= PROTO_MBULK_BIG_ARG {
				if sds.Len(c.querybuf)-c.qbPos <= int(ll)+2 {
					sds.Range(c.querybuf, c.qbPos, -1)
					c.qbPos = 0
					c.querybuf = sds.MakeRoomFor(c.querybuf, int(ll)+2-sds.Len(c.querybuf))
				}
			}
			c.bulkLen = int(ll)
		}

		if sds.Len(c.querybuf)-c.qbPos < c.bulkLen+2 {
			break // 数据不够
		} else {
			if c.qbPos == 0 && c.bulkLen >= PROTO_MBULK_BIG_ARG &&
				sds.Len(c.querybuf) == c.bulkLen+2 {
				c.argv[c.argc] = createObject(ObjString, c.querybuf)
				c.argc++
				c.argvLenSum += c.bulkLen
				sds.IncrLen(c.querybuf, -2) // crlf

				c.querybuf = sds.NewLen(util.Bytes2String(make([]byte, c.bulkLen+2)))
				sds.Clear(c.querybuf)
			} else {
				c.argv[c.argc] = createStringObject(util.Bytes2String(c.querybuf.BufData(c.qbPos)[:c.bulkLen]))
				c.argc++
				c.argvLenSum += c.bulkLen
				c.qbPos += c.bulkLen + 2
			}
			c.bulkLen = -1
			c.multiBulkLen--
		}
	}
	if c.multiBulkLen == 0 {
		return C_OK
	}
	return C_ERR
}

func processCommandAndResetClient(c *Client) error {
	var deadClient error = C_OK
	server.currentClient = c
	if processCommand(c) == C_OK {
		commandProcessed(c)
	}

	if server.currentClient == nil {
		deadClient = C_ERR
	}
	server.currentClient = nil
	return deadClient
}

// todo
func commandProcessed(c *Client) {
	if c.flags&CLIENT_BLOCKED == 0 || c.bType != BLOCKED_MODULE {
		resetClient(c)
	}
}

// todo
func freeClientArgv(c *Client) {
	for i := 0; i < c.argc; i++ {
		//decrRefCount(c.argv[i])
	}

	c.argc = 0
	c.argv = nil
	c.argvLenSum = 0
}

func authRequired(c *Client) bool {
	return (DefaultUser.flags&USER_FLAG_NOPASS == 0 || DefaultUser.flags&USER_FLAG_DISABLED > 0) &&
		!c.authenticated
}

func getClientType(c *Client) int {
	if c.flags&CLIENT_MASTER > 0 {
		return CLIENT_TYPE_MASTER
	}

	if c.flags&CLIENT_SLAVE > 0 && c.flags&CLIENT_MONITOR == 0 {
		return CLIENT_TYPE_SLAVE
	}

	if c.flags&CLIENT_PUBSUB > 0 {
		return CLIENT_TYPE_PUBSUB
	}

	return CLIENT_TYPE_NORMAL
}

// resetClient todo
func resetClient(c *Client) {
	freeClientArgv(c)

	c.reqType = 0
	c.multiBulkLen = 0
	c.bulkLen = -1

	c.flags &= ^CLIENT_REPLY_SKIP
	if c.flags&CLIENT_REPLY_SKIP_NEXT > 0 {
		c.flags |= CLIENT_REPLY_SKIP
		c.flags &= ^CLIENT_REPLY_SKIP_NEXT
	}
}

// clientsArePaused todo
func clientsArePaused() bool {
	return false
}

func clientSetDefaultAuth(c *Client) {
	c.user = DefaultUser
	c.authenticated = c.user.flags&USER_FLAG_NOPASS != 0 && !(c.user.flags&USER_FLAG_DISABLED != 0)
}

func freeClientReplyValue(o interface{}) {

}

// todo
func freeClientAsync(c *Client) {
	syscall.Close(c.conn.Fd)
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
		freeClientAsync(c)
		return
	}

	if server.protectedMode == 1 &&
		server.bindAddrCount == 0 &&
		DefaultUser.flags&USER_FLAG_NOPASS != 0 &&
		c.flags&CLIENT_UNIX_SOCKET == 0 {
		cip := connPeerToString(conn, nil, anet.FdToPeerName)

		if cip != "127.0.0.1" && cip != "::1" {
			const errMsg = `
-DENIED Redis is running in protected mode because protected 
mode is enabled, no bind address was specified, no 
authentication password is requested to clients. In this mode 
connections are only accepted from the loopback interface. 
If you want to connect from external computers to Redis you 
may adopt one of the following solutions: 
1) Just disable protected mode sending the command 
'CONFIG SET protected-mode no' from the loopback interface 
by connecting to Redis from the same host the server is 
running, however MAKE SURE Redis is not publicly accessible 
from internet if you do so. Use CONFIG REWRITE to make this 
change permanent. 
2) Alternatively you can just disable the protected mode by 
editing the Redis configuration file, and setting the protected 
mode option to 'no', and then restarting the server. 
3) If you started the server manually just for testing, restart 
it with the '--protected-mode no' option. 
4) Setup a bind address or an authentication password. 
NOTE: You only need to do one of the above things in order for 
the server to start accepting connections from the outside.
`
			connWrite(c.conn, errMsg)
			server.statRejectedConn++
			freeClientAsync(c)
			return
		}
	}

	server.statNumConnections++
}

/**
postponeClientRead
当我们想要通过线程IO去延迟读取客户端数据时返回true
一般由eventLoop驱动readHandler调用此函数，当此函数
被调用时，客户端将被标记并放到待处理客户端列表中
*/
func postponeClientRead(c *Client) bool {
	return false
}

type ByteArrOrString interface {
	[]byte | string
}

func addReply(c *Client, o *robj) {
	if prepareClientToWrite(c) != C_OK {
		return
	}

	if o.sdsEncodedObject() {
		buf := (*sds.SDS)(o.ptr).BufData(0)
		if _addReplyToBuffer(c, buf) != C_OK {
			_addReplyProtoToList(c, buf)
		}
	} else if o.getEncoding() == ObjEncodingInt {
		buf := fmt.Sprintf("%v", *(*int)(o.ptr))
		if _addReplyToBuffer(c, buf) != C_OK {
			_addReplyProtoToList(c, buf)
		}
	} else {
		panic("Wrong obj->encoding in addReply()")
	}
}

func addReplyNull(c *Client) {
	if c.resp == 2 {
		addReplyProto(c, "$-1\r\n")
	} else {
		addReplyProto(c, "_\r\n")
	}
}

func addReplyBulk(c *Client, o *robj) {
	addReplyBulkLen(c, o)
	addReply(c, o)
	addReply(c, shared.crlf)
}

func addReplyBulkBuffer(c *Client, p []byte, _len int) {
	addReplyLongLongWithPrefix(c, _len, '$')
	addReplyProto(c, p[:_len])
	addReply(c, shared.crlf)
}

func addReplyBulkLongLong(c *Client, vll int64) {
	p := util.String2Bytes(fmt.Sprintf("%d", vll))
	addReplyBulkBuffer(c, p, len(p))
}

func addReplyAggregateLen(c *Client, length int, prefix byte) {
	if prefix == '*' && length < ObjSharedBulkHdrLen {
		addReply(c, shared.mBulkHdr[length])
	} else {
		addReplyLongLongWithPrefix(c, length, prefix)
	}
}

func addReplyMapLen(c *Client, length int) {
	var prefix byte
	if c.resp == 2 {
		prefix = '*'
	} else {
		prefix = '%'
	}
	if c.resp == 2 {
		length *= 2
	}
	addReplyAggregateLen(c, length, prefix)
}

func addReplyArrayLen(c *Client, length int) {
	addReplyAggregateLen(c, length, '*')
}

func addReplyBulkLen(c *Client, o *robj) {
	slen := o.stringObjectLen()
	if slen < ObjSharedBulkHdrLen {
		addReply(c, shared.bulkHdr[slen])
	} else {
		addReplyLongLongWithPrefix(c, slen, '$')
	}
}
func addReplyLongLong(c *Client, ll int) {
	if ll == 0 {
		addReply(c, shared.czero)
	} else if ll == 1 {
		addReply(c, shared.cone)
	} else {
		addReplyLongLongWithPrefix(c, ll, ':')
	}
}
func addReplyLongLongWithPrefix(c *Client, ll int, prefix byte) {
	if prefix == '*' && ll < ObjSharedBulkHdrLen && ll > 0 {
		addReply(c, shared.mBulkHdr[ll])
		return
	}
	if prefix == '$' && ll < ObjSharedBulkHdrLen && ll > 0 {
		addReply(c, shared.bulkHdr[ll])
		return
	}
	lls := fmt.Sprintf("%d", ll)
	buf := make([]byte, 1+len(lls)+2)
	buf[0] = prefix
	copy(buf[1:], lls)
	buf[len(lls)+1] = '\r'
	buf[len(lls)+2] = '\n'
	addReplyProto(c, util.Bytes2String(buf))
}

func _addReplyToBuffer[T ByteArrOrString](c *Client, value T) error {
	available := len(c.buf) - c.bufpos
	if c.flags&CLIENT_CLOSE_AFTER_REPLY > 0 {
		return C_OK
	}

	// 如果有list buffer等回包，则直接加到list中
	if c.reply.Len() > 0 {
		return C_ERR
	}

	// 放不下，放到c.reply中
	if available < len(value) {
		return C_ERR
	}
	copy(c.buf[c.bufpos:], value)
	c.bufpos += len(value)
	return C_OK
}

func addReplyError(c *Client, err string) {
	addReplyErrorLength(c, err)
	afterErrorReply(c, err)
}

func addReplyErrorFormat(c *Client, format string, a ...any) {
	err := fmt.Sprintf(format, a...)
	addReplyErrorLength(c, err)
	afterErrorReply(c, err)
}

func addReplyErrorLength(c *Client, err string) {
	if len(err) == 0 || err[0] != '-' {
		addReplyProto(c, "-ERR ")
	}
	addReplyProto(c, err)
	addReplyProto(c, "\r\n")
}

func addReplyProto[T ByteArrOrString](c *Client, s T) {
	if prepareClientToWrite(c) != C_OK {
		return
	}
	if _addReplyToBuffer(c, s) != C_OK {
		_addReplyProtoToList(c, s)
	}
}

func _addReplyProtoToList[T ByteArrOrString](c *Client, s T) {
	if c.flags&CLIENT_CLOSE_AFTER_REPLY > 0 {
		return
	}

	sLen := len(s)

	tail, ok := c.reply.Last().NodeValue().(*clientReplyBlock)
	if ok {
		avail := cap(tail.buf) - tail.used
		copyLen := avail
		if avail > sLen {
			copyLen = sLen
		}
		copy(tail.buf[tail.used:], s)

		tail.used += copyLen
		s = s[copyLen:]
		sLen = len(s)
	}

	if sLen > 0 {
		alloc := sLen
		if sLen < PROTO_REPLY_CHUNK_BYTES {
			alloc = PROTO_REPLY_CHUNK_BYTES
		}

		tail = &clientReplyBlock{
			used: 0,
			buf:  make([]byte, alloc),
		}

		tail.used = sLen
		copy(tail.buf, s)
		c.reply.AddNodeTail(tail)
		c.replyBytes += cap(tail.buf)
	}

	// asyncCloseClientOnOutputBufferLimitReached todo
}

// afterErrorReply todo
func afterErrorReply[T ByteArrOrString](c *Client, err T) {

}

func prepareClientToWrite(c *Client) error {
	if c.flags&(CLIENT_LUA|CLIENT_MODULE) > 0 {
		return C_OK
	}

	if c.flags&CLIENT_CLOSE_ASAP > 0 {
		return C_ERR
	}

	if c.flags&(CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP) > 0 {
		return C_ERR
	}

	if c.flags&CLIENT_MASTER > 0 && c.flags&CLIENT_MASTER_FORCE_REPLY == 0 {
		return C_ERR
	}

	if c.conn == nil {
		return C_ERR
	}

	if !clientHasPendingReplies(c) && c.flags&CLIENT_PENDING_READ == 0 {
		clientInstallWriteHandler(c)
	}

	return C_OK
}

func clientInstallWriteHandler(c *Client) {
	if c.flags&CLIENT_PENDING_WRITE == 0 && (c.replState == REPL_STATE_NONE ||
		(c.replState == SLAVE_STATE_ONLINE && c.replPutOnlineOnAck > 0)) {
		c.flags |= CLIENT_PENDING_WRITE
		server.clientsPendWrite.AddNodeHead(c)
	}
}

func clientHasPendingReplies(c *Client) bool {
	return c.bufpos > 0 || c.reply.Len() > 0
}

// setProtocolError todo
func setProtocolError(err string, c *Client) {

}

func handleClientsWithPendingWrites() int {
	processed := server.clientsPendWrite.Len()

	iter := server.clientsPendWrite.Rewind()
	for {
		ln := iter.Next()
		if ln == nil {
			break
		}

		c := ln.NodeValue().(*Client)
		c.flags &= ^CLIENT_PENDING_WRITE
		server.clientsPendWrite.DelNode(ln)

		if c.flags&CLIENT_PROTECTED > 0 {
			continue
		}
		if c.flags&CLIENT_CLOSE_ASAP > 0 {
			continue
		}
		if writeToClient(c, 0) == C_ERR {
			continue
		}
		if clientHasPendingReplies(c) {
			aeBarrier := 0
			if server.aofState == aofOn && server.aofFsync == aofFsyncAlways {
				aeBarrier = 1
			}
			if err := connSetWriteHandlerWithBarrier(c.conn, sendReplyToClient, aeBarrier); err != nil {
				freeClientAsync(c)
			}
		}
	}

	return processed
}

func sendReplyToClient(conn *Connection) {
	c := connGetPrivateData(conn).(*Client)
	writeToClient(c, 1)
}

func writeToClient(c *Client, handlerInstalled int) error {
	server.statTotalReadsProcessed++
	totWriten := 0
	nwrite := 0
	for clientHasPendingReplies(c) {
		if c.bufpos > 0 {
			nwrite = connWrite(c.conn, util.Bytes2String(c.buf[c.sentLen:c.bufpos]))
			if nwrite <= 0 {
				break
			}
			c.sentLen += nwrite
			totWriten += nwrite
			if c.sentLen == c.bufpos {
				c.sentLen = 0
				c.bufpos = 0
			}
		} else {
			o := c.reply.First().NodeValue().(*clientReplyBlock)
			objLen := o.used
			if objLen == 0 {
				c.replyBytes -= cap(o.buf)
				c.reply.DelNode(c.reply.First())
				continue
			}
			nwrite = connWrite(c.conn, util.Bytes2String(o.buf[c.sentLen:objLen]))
			if nwrite <= 0 {
				break
			}
			c.sentLen += nwrite
			totWriten += nwrite
			if c.sentLen == objLen {
				c.replyBytes -= cap(o.buf)
				c.reply.DelNode(c.reply.First())
				c.sentLen = 0
				if c.reply.Len() == 0 {
					if c.replyBytes != 0 {
						panic("unexpect reply bytes")
					}
				}
			}
		}

		// todo
		if totWriten > 64*1024 {
			break
		}
	}

	server.statNetOutputBytes += totWriten
	if nwrite == -1 {
		if connGetState(c.conn) == CONN_STATE_CONNECTED {
			nwrite = 0
		} else {
			log.Printf("Error write to client: %v\n", c.conn.LastErr)
			freeClientAsync(c)
			return C_ERR
		}
	}
	if totWriten > 0 {
		if c.flags&CLIENT_MASTER > 0 {
			c.lastInteraction = server.unixtime
		}
	}
	if !clientHasPendingReplies(c) {
		c.sentLen = 0
		if handlerInstalled == 1 {
			connSetWriteHandlerWithBarrier(c.conn, nil, 0)
		}
		if c.flags&CLIENT_CLOSE_AFTER_REPLY > 0 {
			freeClientAsync(c)
			return C_ERR
		}
	}
	return C_OK
}

func rewriteClientCommandVector(c *Client, argc int, argv ...*robj) {
	c.argv = argv
	c.argc = argc
	c.argvLenSum = 0
	for j := 0; j < c.argc; j++ {
		if c.argv[j] != nil {
			c.argvLenSum += getStringObjectLen(c.argv[j])
		}
		c.cmd = lookupCommandOrOriginal(c.argv[0].ptr)
	}
	if c.cmd == nil {
		panic("cmd nil")
	}
}

func getStringObjectLen(o *robj) int {
	if o.getType() != ObjString {
		panic(fmt.Sprintf("expect objString(0), but %d", o.getType()))
	}
	if o.getEncoding() == ObjEncodingRaw || o.getEncoding() == ObjEncodingEmbStr {
		return sds.Len(*(*sds.SDS)(o.ptr))
	}
	return 0
}

func addReplyDeferredLen(c *Client) *adlist.ListNode {
	if prepareClientToWrite(c) != C_OK {
		return nil
	}
	trimReplyUnusedTailSpace(c)
	c.reply.AddNodeTail(nil)
	return c.reply.Last()
}

func trimReplyUnusedTailSpace(c *Client) {
	ln := c.reply.Last()
	var tail *clientReplyBlock
	if ln != nil {
		tail = ln.NodeValue().(*clientReplyBlock)
	}

	if tail == nil {
		return
	}

	if len(tail.buf)-tail.used > len(tail.buf)/4 && tail.used < PROTO_REPLY_CHUNK_BYTES {
		oldSize := tail.used
		buf := make([]byte, tail.used)
		copy(buf, tail.buf)
		tail.buf = buf
		c.replyBytes = c.replyBytes + tail.used - oldSize
	}
}

func setDeferredSetLen(c *Client, node *adlist.ListNode, length int) {
	prefix := byte('*')
	if c.resp != 2 {
		prefix = '~'
	}
	setDeferredAggregateLen(c, node, length, prefix)
}

func setDeferredAggregateLen(c *Client, node *adlist.ListNode, length int, prefix byte) {
	var next *clientReplyBlock
	lenStr := fmt.Sprintf("%c%d\r\n", prefix, length)

	if node == nil {
		return
	}
	var ok bool
	if next, ok = node.Next().NodeValue().(*clientReplyBlock); ok &&
		len(next.buf)-next.used >= len(lenStr) &&
		next.used < PROTO_REPLY_CHUNK_BYTES*4 {
		copy(next.buf[len(lenStr):], next.buf[:next.used])
		copy(next.buf, lenStr)
		next.used += len(lenStr)
		c.reply.DelNode(node)
	} else {
		buf := new(clientReplyBlock)
		buf.buf = make([]byte, len(lenStr))
		buf.used = len(lenStr)
		copy(buf.buf, lenStr)
		node.SetNodeValue(buf)
		c.replyBytes += cap(buf.buf)
	}

	asyncCloseClientOnOutputBufferLimitReached(c)
}

func asyncCloseClientOnOutputBufferLimitReached(c *Client) {
	if c.conn == nil {
		return
	}

	if c.replyBytes == 0 || c.flags&CLIENT_CLOSE_ASAP > 0 {
		return
	}

	if checkClientOutputBufferLimits(c) {
		freeClientAsync(c)
		log.Printf("Client scheduled to be closed ASAP for overcoming of output buffer limits: %d", c.replyBytes)
	}
}

// todo
func checkClientOutputBufferLimits(c *Client) bool {
	return false
}
