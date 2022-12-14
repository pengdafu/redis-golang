package main

import (
	"errors"
	"fmt"
	"github.com/pengdafu/redis-golang/adlist"
	"github.com/pengdafu/redis-golang/ae"
	"github.com/pengdafu/redis-golang/anet"
	"github.com/pengdafu/redis-golang/sds"
	"github.com/pengdafu/redis-golang/util"
	"log"
	"os"
	"strings"
	"syscall"
	"time"
)

const (
	CONFIG_MIN_RESERVED_FDS = 32
	CONFIG_FDSET_INCR       = CONFIG_MIN_RESERVED_FDS + 96
	CONFIG_BINDADDR_MAX     = 16
	CONFIG_RUN_ID_SIZE      = 40
)

const (
	PROTO_REPLY_CHUNK_BYTES = 16 * 1024
	PROTO_IOBUF_LEN
	PROTO_INLINE_MAX_SIZE
	PROTO_MBULK_BIG_ARG = 32 * 1024
)

const (
	PROTO_REQ_INLINE    = 1
	PROTO_REQ_MULTIBULK = 2
)

const (
	USER_FLAG_ENABLED = 1 << iota
	USER_FLAG_DISABLED
	USER_FLAG_ALLKEYS
	USER_FLAG_ALLCOMMANDS
	USER_FLAG_NOPASS
	USER_FLAG_ALLCHANNELS
	USER_FLAG_SANITIZE_PAYLOAD
	USER_FLAG_SANITIZE_PAYLOAD_SKIP
)

const (
	REPL_STATE_NONE = iota
	REPL_STATE_CONNECT
	REPL_STATE_CONNECTING
	REPL_STATE_RECEIVE_PING_REPLY
	REPL_STATE_SEND_HANDSHAKE
	REPL_STATE_RECEIVE_AUTH_REPLY
	REPL_STATE_RECEIVE_PORT_REPLY
	REPL_STATE_RECEIVE_IP_REPLY
	REPL_STATE_RECEIVE_CAPA_REPLY
	REPL_STATE_SEND_PSYNC
	REPL_STATE_RECEIVE_PSYNC_REPLY
	REPL_STATE_TRANSFER
	REPL_STATE_CONNECTED
)

const (
	SLAVE_CAPA_NONE = 0
	SLAVE_CAPA_EOF  = 1 << iota
	SLAVE_CAPA_PSYNC2
)

const (
	SLAVE_STATE_ONLINE = 9
)

const (
	BLOCKED_NONE = iota
	BLOCKED_LIST
	BLOCKED_WAIT
	BLOCKED_MODULE
	BLOCKED_STREAM
	BLOCKED_ZSET
	BLOCKED_PAUSE
	BLOCKED_NUM
)

const (
	CLIENT_TYPE_NORMAL = iota
	CLIENT_TYPE_SLAVE
	CLIENT_TYPE_PUBSUB
	CLIENT_TYPE_MASTER
	CLIENT_TYPE_COUNT
	CLIENT_TYPE_OBUF_COUNT = 3
)

const (
	CLIENT_SLAVE = 1 << iota
	CLIENT_MASTER
	CLIENT_MONITOR
	CLIENT_MULTI
	CLIENT_BLOCKED
	CLIENT_DIRTY_CAS
	CLIENT_CLOSE_AFTER_REPLY
	CLIENT_UNBLOCKED
	CLIENT_LUA
	CLIENT_ASKING
	CLIENT_CLOSE_ASAP
	CLIENT_UNIX_SOCKET
	CLIENT_DIRTY_EXEC
	CLIENT_MASTER_FORCE_REPLY
	CLIENT_FORCE_AOF
	CLIENT_FORCE_REPL
	CLIENT_PRE_PSYNC
	CLIENT_READONLY
	CLIENT_PUBSUB
	CLIENT_PREVENT_AOF_PROP
	CLIENT_PREVENT_REPL_PROP
	CLIENT_PENDING_WRITE
	CLIENT_REPLY_OFF
	CLIENT_REPLY_SKIP_NEXT
	CLIENT_REPLY_SKIP
	CLIENT_LUA_DEBUG
	CLIENT_LUA_DEBUG_SYNC
	CLIENT_MODULE
	CLIENT_PROTECTED
	CLIENT_PENDING_READ
	CLIENT_PENDING_COMMAND
	CLIENT_TRACKING
	CLIENT_TRACKING_BROKEN_REDIR
	CLIENT_TRACKING_BCAST
	CLIENT_TRACKING_OPTIN
	CLIENT_TRACKING_OPTOUT
	CLIENT_TRACKING_CACHING
	CLIENT_TRACKING_NOLOOP
	CLIENT_IN_TO_TABLE
	CLIENT_PROTOCOL_ERROR
	CLIENT_CLOSE_AFTER_COMMAND
	CLIENT_DENY_BLOCKING
	CLIENT_REPL_RDBONLY
	CLIENT_PREVENT_PROP = CLIENT_PREVENT_AOF_PROP | CLIENT_PREVENT_REPL_PROP
)

const (
	MaxMemoryFlagLru = 1 << 0
	MaxMemoryFlagLfu = 1 << 1
)

// server static configuration
const (
	ObjSharedIntegers     = 10000
	ObjSharedBulkHdrLen   = 32
	ProtoSharedSelectCmds = 10
)

var (
	C_OK  error = nil
	C_ERR error = errors.New("error")
)

// RedisModuleUserChangedFunc 会在每次调用moduleNotifyUserChanged()函数后执行
/**
 * a user authenticated via the module API is associated with a different
 * user or gets disconnected. This needs to be exposed since you can't cast
 * a function pointer to (void *).
 */
type RedisModuleUserChangedFunc func(clientId uint64, privateData interface{})

type socketFds struct {
	fd    [CONFIG_BINDADDR_MAX]int
	count int
}
type RedisServer struct {
	el *ae.EventLoop

	protectedMode int

	port          int
	ipfd          *socketFds
	tcpBacklog    int
	bindAddr      [CONFIG_BINDADDR_MAX]string
	bindAddrCount int

	clients                 []*Client
	currentClient           *Client
	clusterEnabled          bool
	statRejectedConn        uint64 // 拒绝客户端连接的次数
	statNumConnections      uint64 // 成功连接客户端的次数
	statTotalReadsProcessed uint64 // 成功处理read的次数
	tcpKeepalive            int
	maxclients              int
	protoMaxBulkLen         int64
	clientMaxQueryBufLen    int
	maxMemoryPolicy         int

	lruClock uint32
	hz       uint32

	dbnum        int
	db           []*redisDb
	nextClientId uint64

	unixtime   int64
	luaTimeout uint32

	clientsPendWrite *adlist.List
}

var server *RedisServer

type Client struct {
	id              uint64 // 自增唯一ID
	conn            *Connection
	resp            int // resp 协议版本，可以是2或者3
	db              *redisDb
	name            *robj   // 客户端名字，通过SETNAME设置
	querybuf        sds.SDS // 缓存客户端请求的buf
	qbPos           int     // querybuf 读到的位置
	pendingQueryBuf sds.SDS // 如果此客户端被标记为主服务器，则此缓冲区表示从主服务器接收的复制流中尚未应用的部分。

	querybufPeak int           // 最近100ms或者更长时间querybuf的峰值
	argc         int           // 当前command有多少个参数
	argv         []*robj       // 当前command的参数列表
	originalArgc int           // 如果参数被重写，原始参数的个数
	originalArgv []*robj       // 如果参数被重写，原始的参数列表
	argvLenSum   int           // len(argv)
	cmd, lastCmd *redisCommand // 最后一次执行的command
	user         *user         // user与connect关联，如果为nil，则代表是admin，可以做任何操作

	reqType                   uint8 // request protocol type: PROTO_REQ_*
	multiBulkLen              int   // 要读取的多个批量参数的数量
	bulkLen                   int   // 批量请求的参数长度
	reply                     *adlist.List
	replyBytes                uint64 // 要响应的字节长度
	sentLen                   uint64 // 当前缓冲区或者正在发送中的对象已经发送的字节数
	ctime                     int64  // 客户端创建时间
	duration                  int64  // 当前command的运行时间，用来阻塞或非阻塞命令的延迟
	lastInteraction           int64  // 上次交互时间，用于超时，单位秒
	obufSoftLimitReachedTime  time.Duration
	flags                     int                          // 客户端的flag，CLIENT_* 宏定义
	authenticated             bool                         // 当默认用户需要认证
	replState                 int                          // 如果client是一个从节点，则为从节点的复制状态
	replPutOnlineOnAck        int                          // 在第一个ACK的时候，安装从节点的写处理器
	replDBFd                  int                          // 复制database 的文件描述符
	replDBOff                 int                          // 复制database的文件的偏移
	replDBSize                int                          // 复制db的文件的大小
	replPreamble              int                          // 复制DB序言
	readReplOff               int                          // Read replication offset if this is a master
	replOff                   int                          // Applied replication offset if this is a master
	replAckOff                int                          // Replication ack offset, if this is a slave
	replAckTime               int64                        // Replication ack time, if this is a slave
	psyncInitialOffset        int                          // FULLRESYNC reply offset other slaves copying this slave output buffer should use.
	replId                    [CONFIG_RUN_ID_SIZE + 1]byte // 主复制Id，如果是主节点
	slaveListeningPort        int                          // As configured with: REPLCONF listening-port
	slaveAddr                 string                       // Optionally given by REPLCONF ip-address
	slaveCapa                 int                          // 从节点容量：SLAVE_CAPA_* bitwise OR
	mstate                    multiState                   // MULTI/EXEC state
	bType                     int                          // 如果是CLIENT_BLOCKED类型，表示阻塞
	bpop                      blockingState                // blocking state
	woff                      int                          // 最后一次写的全局复制偏移量
	watchedKeys               *adlist.List                 // Keys WATCHED for MULTI/EXEC CAS
	pubSubChannels            *dict                        // 客户端关注的渠道(SUBSCRIBE)
	pubSubPatterns            *adlist.List                 // 客户端关注的模式(SUBSCRIBE)
	peerId                    sds.SDS                      // Cached peer ID
	sockName                  sds.SDS                      // Cached connection target address.
	clientListNode            *adlist.ListNode             //list node in client list
	pausedListNode            *adlist.ListNode             //list node within the pause list
	authCallback              RedisModuleUserChangedFunc   // 当认证的用户被改变是，回调模块将被执行
	authCallbackPrivdata      interface{}                  // 当auth回调被执行的时候，该值当参数传递过去
	authModule                interface{}                  // 拥有回调函数的模块，当模块被卸载进行清理时，该模块用于断开客户端。不透明的Redis核心
	clientTrackingRedirection uint64                       // 如果处于追踪模式并且该字段不为0，那么该客户端获取keys的无效信息，将会发送到特殊的clientId
	clientTrackingPrefixes    *rax                         // 在客户端缓存上下文中，我们在BCAST模式下已经订阅的前缀字典
	clientCronLastMemoryUsage uint64                       //
	clientCronLastMemoryType  int
	// response buf
	bufpos int
	buf    [PROTO_REPLY_CHUNK_BYTES]byte
}
type redisDb struct {
	// todo redisDb
}
type redisCommand struct {
}
type user struct {
	flags uint64
}
type multiState struct {
}
type blockingState struct {
	timeout time.Duration
	keys    *dict
	target  *robj
	listPos struct {
		wherefrom int
		whereto   int
	}

	xReadCount      int
	xReadGroup      *robj
	xReadConsumer   *robj
	xReadGroupNoAck int

	numReplicas         int
	replOffset          uint
	moduleBlockedHandle interface{}
}

func New() *RedisServer {
	server = new(RedisServer)
	return server
}

func (server *RedisServer) Init() {
	var err error

	server.ipfd = new(socketFds)
	server.port = 6380
	server.bindAddr[0] = "127.0.0.1"
	server.bindAddrCount = 1
	server.maxclients = 100
	server.clientsPendWrite = adlist.Create()

	createSharedObjects()

	// 创建aeEventLoop
	server.el, err = ae.CreateEventLoop(server.maxclients + CONFIG_FDSET_INCR)
	if err != nil {
		panic(fmt.Sprintf("create aeEventLoop err: %v", err))
	}

	if server.port != 0 {
		if err := server.listenToPort(server.port, server.ipfd); err != C_OK {
			log.Println(err)
			os.Exit(1)
		}
	}

	// 创建aeTimeEvent
	if err := server.el.CreateTimeEvent(1, server.serverCron, nil, nil); err == ae.ERR {
		panic("Can't create event loop timer.")
	}

	// 创建连接处理
	if server.createSocketAcceptHandler(server.ipfd, acceptTcpHandler) != C_OK {
		panic("Unrecoverable error creating TCP socket accept handler.")
	}
}

func (server *RedisServer) Start() {
	server.el.AeMain()
}

func (server *RedisServer) Stop() {
	server.el.Stop = 1
	for i := 0; i < server.ipfd.count; i++ {
		syscall.Close(server.ipfd.fd[i])
	}
}

func (server *RedisServer) serverCron(el *ae.EventLoop, id uint64, clientData interface{}) int {
	return 0
}

func (server *RedisServer) createSocketAcceptHandler(sfd *socketFds, accessHandle ae.FileProc) error {
	for i := 0; i < sfd.count; i++ {
		if err := server.el.AeCreateFileEvent(sfd.fd[i], ae.Readable, accessHandle, nil); err != nil {
			for j := i - 1; j >= 0; j-- {
				server.el.AeDeleteFileEvent(sfd.fd[j], ae.Readable)
			}
		}
	}
	return nil
}

func (server *RedisServer) listenToPort(port int, sfd *socketFds) (err error) {
	bindAddr := server.bindAddr
	bindAddrCount := server.bindAddrCount
	defaultBindAddr := [2]string{"*", "-::*"}

	if server.bindAddrCount == 0 {
		bindAddrCount = 2
		bindAddr[0], bindAddr[1] = defaultBindAddr[0], defaultBindAddr[1]
	}

	for j := 0; j < bindAddrCount; j++ {
		addr := bindAddr[j]
		if strings.Contains(addr, ":") {
			sfd.fd[sfd.count], err = anet.Tcp6Server(port, addr, server.tcpBacklog)
		} else {
			sfd.fd[sfd.count], err = anet.TcpServer(port, addr, server.tcpBacklog)
		}
		if err != nil {
			server.closeSocketListeners(sfd)
			return err
		}
		_ = anet.NonBlock(sfd.fd[sfd.count])
		_ = anet.Cloexec(sfd.fd[sfd.count])
		sfd.count++
	}
	return nil
}

func (server *RedisServer) closeSocketListeners(sfd *socketFds) {
	for i := 0; i < sfd.count; i++ {
		if sfd.fd[i] == -1 {
			continue
		}
		server.el.AeDeleteFileEvent(sfd.fd[i], ae.Readable)
	}
	sfd.count = 0
}

func (server *RedisServer) connSocketClose(conn *Connection) {

}

// clientReplyBlock.size = cap(buf)
type clientReplyBlock struct {
	//size int
	used int
	buf  []byte
}

func processCommand(c *Client) error {
	moduleCallCommandFilters(c)

	if util.Bytes2String(c.argv[0].ptr.(sds.SDS).Offset(0)) == "quit" {
		addReply(c, shared.ok)
		c.flags |= CLIENT_CLOSE_AFTER_REPLY
		return C_ERR
	}

	c.cmd = lookupCommand(c.argv[0].ptr.(sds.SDS))
}

type shareObject struct {
	crlf, ok, err, emptybulk, czero, cone, pong, space                 *robj
	colon, queue                                                       *robj
	null, nullArray, emptyMap, emptySet                                [4]*robj
	emptyArray, wrongTypeErr, noKeyErr, syntaxErr, sameObjectErr       *robj
	outOfRangeErr, noScriptErr, loadingErr, slowScriptErr, bgSaveErr   *robj
	masterDownErr, roSlaveErr, execAbortErr, noAuthErr, noReplicateErr *robj
	busyKeyErr, oomErr, plus, messageBulk, pMessageBulk, subscribeBulk *robj
	unsubscribeBulk, pSubscribeBulk, pUnsubscribeBulk, del, unlink     *robj
	rpop, lpop, lpush, rpoplpush, zpopmin, zpopmax, emptyScan          *robj
	multi, exec                                                        *robj
	selec                                                              [ProtoSharedSelectCmds]*robj
	integers                                                           [ObjSharedIntegers]*robj
	mBulkHdr                                                           [ObjSharedBulkHdrLen]*robj
	bulkHdr                                                            [ObjSharedBulkHdrLen]*robj
	minString, maxString                                               sds.SDS
}

var shared shareObject

func createSharedObjects() {
	shared.crlf = createObject(ObjString, sds.NewLen("\r\n"))
	shared.ok = createObject(ObjString, sds.NewLen("+OK\r\n"))
	shared.err = createObject(ObjString, sds.NewLen("-ERR\r\n"))
	shared.emptybulk = createObject(ObjString, sds.NewLen("$0\r\n\r\n"))
	shared.czero = createObject(ObjString, sds.NewLen(":0\r\n"))
	shared.cone = createObject(ObjString, sds.NewLen(":1\r\n"))
	shared.emptyArray = createObject(ObjString, sds.NewLen("*0\r\n"))
	shared.pong = createObject(ObjString, "+PONG\r\n")
	shared.queue = createObject(ObjString, "+QUEUED\r\n")
	shared.emptyScan = createObject(ObjString, "*2\r\n$1\r\n0\r\n*0\r\n")
	shared.wrongTypeErr = createObject(ObjString, sds.NewLen(
		"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
	shared.noKeyErr = createObject(ObjString, sds.NewLen("-ERR no such key\r\n"))
	shared.syntaxErr = createObject(ObjString, sds.NewLen(
		"-ERR syntax error\r\n"))
	shared.sameObjectErr = createObject(ObjString, sds.NewLen(
		"-ERR source and destination objects are the same\r\n"))
	shared.outOfRangeErr = createObject(ObjString, sds.NewLen(
		"-ERR index out of range\r\n"))
	shared.noScriptErr = createObject(ObjString, sds.NewLen(
		"-NOSCRIPT No matching script. Please use EVAL.\r\n"))
	shared.loadingErr = createObject(ObjString, sds.NewLen(
		"-LOADING Redis is loading the dataset in memory\r\n"))
	shared.slowScriptErr = createObject(ObjString, sds.NewLen(
		"-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"))
	shared.masterDownErr = createObject(ObjString, sds.NewLen(
		"-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n"))
	shared.bgSaveErr = createObject(ObjString, sds.NewLen(
		"-MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.\r\n"))
	shared.roSlaveErr = createObject(ObjString, sds.NewLen(
		"-READONLY You can't write against a read only replica.\r\n"))
	shared.noAuthErr = createObject(ObjString, sds.NewLen(
		"-NOAUTH Authentication required.\r\n"))
	shared.oomErr = createObject(ObjString, sds.NewLen(
		"-OOM command not allowed when used memory > 'maxmemory'.\r\n"))
	shared.execAbortErr = createObject(ObjString, sds.NewLen(
		"-EXECABORT Transaction discarded because of previous errors.\r\n"))
	shared.noReplicateErr = createObject(ObjString, sds.NewLen(
		"-NOREPLICAS Not enough good replicas to write.\r\n"))
	shared.busyKeyErr = createObject(ObjString, sds.NewLen(
		"-BUSYKEY Target key name already exists.\r\n"))
	shared.space = createObject(ObjString, sds.NewLen(" "))
	shared.colon = createObject(ObjString, sds.NewLen(":"))
	shared.plus = createObject(ObjString, sds.NewLen("+"))

	/* The shared NULL depends on the protocol version. */
	shared.null[0] = nil
	shared.null[1] = nil
	shared.null[2] = createObject(ObjString, sds.NewLen("$-1\r\n"))
	shared.null[3] = createObject(ObjString, sds.NewLen("_\r\n"))

	shared.nullArray[0] = nil
	shared.nullArray[1] = nil
	shared.nullArray[2] = createObject(ObjString, sds.NewLen("*-1\r\n"))
	shared.nullArray[3] = createObject(ObjString, sds.NewLen("_\r\n"))

	shared.emptyMap[0] = nil
	shared.emptyMap[1] = nil
	shared.emptyMap[2] = createObject(ObjString, sds.NewLen("*0\r\n"))
	shared.emptyMap[3] = createObject(ObjString, sds.NewLen("%0\r\n"))

	shared.emptySet[0] = nil
	shared.emptySet[1] = nil
	shared.emptySet[2] = createObject(ObjString, sds.NewLen("*0\r\n"))
	shared.emptySet[3] = createObject(ObjString, sds.NewLen("~0\r\n"))

	for j := 0; j < ProtoSharedSelectCmds; j++ {
		dictIdStr := fmt.Sprintf("%d", j)
		shared.selec[j] = createObject(ObjString,
			sds.NewLen(fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", len(dictIdStr), dictIdStr)))
	}
	shared.messageBulk = createStringObject("$7\r\nmessage\r\n")
	shared.pMessageBulk = createStringObject("$8\r\npmessage\r\n")
	shared.subscribeBulk = createStringObject("$9\r\nsubscribe\r\n")
	shared.unsubscribeBulk = createStringObject("$11\r\nunsubscribe\r\n")
	shared.pSubscribeBulk = createStringObject("$10\r\npsubscribe\r\n")
	shared.pUnsubscribeBulk = createStringObject("$12\r\npunsubscribe\r\n")
	shared.del = createStringObject("DEL")
	shared.unlink = createStringObject("UNLINK")
	shared.rpop = createStringObject("RPOP")
	shared.lpop = createStringObject("LPOP")
	shared.lpush = createStringObject("LPUSH")
	shared.rpoplpush = createStringObject("RPOPLPUSH")
	shared.zpopmin = createStringObject("ZPOPMIN")
	shared.zpopmax = createStringObject("ZPOPMAX")
	shared.multi = createStringObject("MULTI")
	shared.exec = createStringObject("EXEC")
	for j := 0; j < ObjSharedIntegers; j++ {
		shared.integers[j] = createObject(ObjString, j).makeObjectShared()
		shared.integers[j].setEncoding(ObjEncodingInt)
	}
	for j := 0; j < ObjSharedBulkHdrLen; j++ {
		shared.mBulkHdr[j] = createObject(ObjString, fmt.Sprintf("*%d\r\n", j))
		shared.bulkHdr[j] = createObject(ObjString, fmt.Sprintf("$%d\r\n", j))
	}
	/* The following two shared objects, minstring and maxstrings, are not
	 * actually used for their value but as a special object meaning
	 * respectively the minimum possible string and the maximum possible
	 * string in string comparisons for the ZRANGEBYLEX command. */
	shared.minString = sds.NewLen("minstring")
	shared.maxString = sds.NewLen("maxstring")
}
