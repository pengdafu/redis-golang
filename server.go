package main

import (
	"errors"
	"fmt"
	"github.com/pengdafu/redis-golang/adlist"
	"github.com/pengdafu/redis-golang/ae"
	"github.com/pengdafu/redis-golang/anet"
	"github.com/pengdafu/redis-golang/dict"
	"github.com/pengdafu/redis-golang/sds"
	"github.com/pengdafu/redis-golang/util"
	"log"
	"os"
	"strings"
	"time"
	"unsafe"
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
	MaxMemoryFlagLru              = 1 << 0
	MaxMemoryFlagLfu              = 1 << 1
	MaxMemoryFlagAllKeys          = 1 << 2
	MaxMemoryFlagNoSharedIntegers = MaxMemoryFlagLru | MaxMemoryFlagLfu
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

	commands     *dict.Dict
	origCommands *dict.Dict

	protectedMode int

	port          int
	ipfd          *socketFds
	tcpBacklog    int
	bindAddr      [CONFIG_BINDADDR_MAX]string
	bindAddrCount int

	clients                 []*Client
	currentClient           *Client
	clusterEnabled          bool
	lazyFreeLazyUserDel     bool
	statRejectedConn        uint64 // 拒绝客户端连接的次数
	statNumConnections      uint64 // 成功连接客户端的次数
	statTotalReadsProcessed uint64 // 成功处理read的次数
	tcpKeepalive            int
	maxclients              int
	protoMaxBulkLen         int64
	clientMaxQueryBufLen    int
	dirty                   int
	maxMemoryPolicy         int
	maxMemory               int64

	lruClock uint32
	hz       float32

	dbnum        int
	db           []*redisDb
	nextClientId uint64

	unixtime   int64
	ustime     int64 // 微秒
	luaTimeout uint32

	clientsPendWrite   *adlist.List
	readyKeys          *adlist.List
	aofState           int
	aofFsync           int
	statNetOutputBytes int

	rdbChildPid, aofChildPid, moduleChildPid int
}

const (
	aofOff = iota
	aofOn
	aofWaitRewrite
)
const (
	aofFsyncOn = iota
	aofFsyncAlways
	aofFsyncEverySec
)

const (
	unitSeconds      = 0
	unitMilliSeconds = 1
)

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
	replyBytes                int   // 要响应的字节长度
	sentLen                   int   // 当前缓冲区或者正在发送中的对象已经发送的字节数
	ctime                     int64 // 客户端创建时间
	duration                  int64 // 当前command的运行时间，用来阻塞或非阻塞命令的延迟
	lastInteraction           int64 // 上次交互时间，用于超时，单位秒
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
	pubSubChannels            *dict.Dict                   // 客户端关注的渠道(SUBSCRIBE)
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

const MaxKeysBuffer = 256

type getKeysResult struct {
	keysBuf [MaxKeysBuffer]int
	keys    []int
	numKeys int
	size    int
}
type RedisCommandProc func(c *Client)
type RedisGetKeysProc func(cmd *redisCommand, argv []*robj, argc int) (*getKeysResult, error)
type redisCommand struct {
	name                       string
	proc                       RedisCommandProc
	arity                      int
	sflags                     string
	flags                      uint64
	getKeysProc                RedisGetKeysProc
	firstKey, lastKey, keyStep int
	microseconds, calls        uint64
	id                         int
}
type user struct {
	flags uint64
}
type multiState struct {
	cmdFlags uint64
}
type blockingState struct {
	timeout time.Duration
	keys    *dict.Dict
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

func (server *RedisServer) InitServer() {
	dict.SetHashFunctionSeed(util.GetRandomBytes(16))

	initServerConfig()

	var err error

	createSharedObjects()

	// 创建aeEventLoop
	server.el, err = ae.CreateEventLoop(server.maxclients + CONFIG_FDSET_INCR)
	if err != nil {
		panic(fmt.Sprintf("create aeEventLoop err: %v", err))
	}
	server.db = make([]*redisDb, server.dbnum)
	for i := 0; i < server.dbnum; i++ {
		db := &redisDb{}
		db.dict = dict.Create(dbDictType, nil)
		db.expires = dict.Create(keyPtrDictType, nil)
		db.blockingKeys = dict.Create(keyListDictType, nil)
		db.watchedKeys = dict.Create(keyListDictType, nil)
		db.readyKeys = dict.Create(objectKeyPointValueDictType, nil)
		db.id = i
		db.defragLater = adlist.Create()
		server.db[i] = db
	}

	server.el.AeSetBeforeSleepProc(beforeSleep)

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

func initServerConfig() {
	server.ipfd = new(socketFds)
	server.port = 6380
	server.bindAddr[0] = "127.0.0.1"
	server.bindAddrCount = 1
	server.maxclients = 100
	server.clientsPendWrite = adlist.Create()
	server.readyKeys = adlist.Create()
	server.hz = 0.5
	server.clientMaxQueryBufLen = 1024 * 1024
	server.dbnum = 16
	server.protoMaxBulkLen = 1024 * 1024

	server.commands = dict.Create(commandTableDictType, nil)
	server.origCommands = dict.Create(commandTableDictType, nil)
	populateCommandTable()
}

func (server *RedisServer) Start() {
	server.el.AeMain()
}

func (server *RedisServer) Stop() {
	server.el.AeDeleteEventLoop()
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

var redisCommandTable = []redisCommand{
	{"module", moduleCommand, -2,
		"admin no-script",
		0, nil, 0, 0, 0, 0, 0, 0},

	{"get", getCommand, 2,
		"read-only fast @string",
		0, nil, 1, 1, 1, 0, 0, 0},

	/* Note that we can't flag set as fast, since it may perform an
	 * implicit DEL of a large key. */
	{"set", setCommand, -3,
		"write use-memory @string",
		0, nil, 1, 1, 1, 0, 0, 0},
	{"exec", execCommand, 1,
		"no-script no-monitor no-slowlog ok-loading ok-stale @transaction",
		0, nil, 0, 0, 0, 0, 0, 0},
	{"del", delCommand, -2,
		"write @keyspace",
		0, nil, 1, -1, 1, 0, 0, 0},
	{"unlink", unlinkCommand, -2,
		"write fast @keyspace",
		0, nil, 1, -1, 1, 0, 0, 0},
}

func populateCommandTable() {
	for i := 0; i < len(redisCommandTable); i++ {
		c := &redisCommandTable[i]
		if populateCommandTableParseFlags(c, c.sflags) != C_OK {
			panic("Unsupported command flag")
		}

		c.id = ACLGetCommandId(c.name)
		s := sds.NewLen(c.name)
		if server.commands.Add(unsafe.Pointer(&s), unsafe.Pointer(c)) &&
			server.origCommands.Add(unsafe.Pointer(&s), unsafe.Pointer(c)) {
			continue
		}
		panic("add command to server.commands err")
	}
}

// todo rax tree
var m = map[string]int{}
var commandId int

func ACLGetCommandId(name string) int {
	if id, ok := m[name]; ok {
		return id
	}
	commandId++
	m[name] = commandId
	return commandId
}

const (
	CmdWrite = 1 << iota
	CmdReadOnly
	CmdDenyOom
	CmdModule
	CmdAdmin
	CmdPubSub
	CmdNoScript
	CmdRandom
	CmdSortForScript
	CmdLoading
	CmdStale
	CmdSkipMonitor
	CmdSkipSlowLog
	CmdAsking
	CmdFast
	CmdNoAuth
	CmdModuleGetKeys
	CmdModuleNoCluster
	CmdCategoryKeySpace
	CmdCategoryRead
	CmdCategoryWrite
	CmdCategorySet
	CmdCategorySortedSet
	CmdCategoryList
	CmdCategoryHash
	CmdCategoryString
	CmdCategoryBitmap
	CmdCategoryHyperloglog
	CmdCategoryGeo
	CmdCategoryStream
	CmdCategoryPubSub
	CmdCategoryAdmin
	CmdCategoryFast
	CmdCategorySlow
	CmdCategoryBlocking
	CmdCategoryDangerous
	CmdCategoryConnection
	CmdCategoryTransaction
	CmdCategoryScripting
)

func populateCommandTableParseFlags(c *redisCommand, sflags string) error {
	argv, argc := sds.SplitArgs(sds.NewLen(sflags))
	if argv == nil {
		return C_ERR
	}

	for i := 0; i < argc; i++ {
		flag := argv[i].BufData(0)
		if util.StrCmp(flag, "write") {
			c.flags |= CmdWrite | CmdCategoryWrite
		} else if util.StrCmp(flag, "read-only") {
			c.flags |= CmdReadOnly | CmdCategoryRead
		} else if util.StrCmp(flag, "use-memory") {
			c.flags |= CmdDenyOom
		} else if util.StrCmp(flag, "admin") {
			c.flags |= CmdAdmin | CmdCategoryAdmin | CmdCategoryDangerous
		} else if util.StrCmp(flag, "pubsub") {
			c.flags |= CmdPubSub | CmdCategoryPubSub
		} else if util.StrCmp(flag, "no-script") {
			c.flags |= CmdNoScript
		} else if util.StrCmp(flag, "random") {
			c.flags |= CmdRandom
		} else if util.StrCmp(flag, "to-sort") {
			c.flags |= CmdSortForScript
		} else if util.StrCmp(flag, "ok-loading") {
			c.flags |= CmdLoading
		} else if util.StrCmp(flag, "ok-stale") {
			c.flags |= CmdStale
		} else if util.StrCmp(flag, "no-monitor") {
			c.flags |= CmdSkipMonitor
		} else if util.StrCmp(flag, "no-slowlog") {
			c.flags |= CmdSkipSlowLog
		} else if util.StrCmp(flag, "cluster-asking") {
			c.flags |= CmdAsking
		} else if util.StrCmp(flag, "fast") {
			c.flags |= CmdFast | CmdCategoryFast
		} else if util.StrCmp(flag, "no-auth") {
			c.flags |= CmdNoAuth
		} else {
			if catflag := ACLGetCommandCategoryFlagByName(string(flag[1:])); catflag > 0 && flag[0] == '@' {
				c.flags |= catflag
			} else {
				return C_ERR
			}
		}
	}

	if c.flags&CmdCategoryFast == 0 {
		c.flags |= CmdCategorySlow
	}
	return C_OK
}

var ACLCommandCategories = []struct {
	name string
	flag uint64
}{
	{"keyspace", CmdCategoryKeySpace},
	{"read", CmdCategoryRead},
	{"write", CmdCategoryWrite},
	{"set", CmdCategorySet},
	{"sortedset", CmdCategorySortedSet},
	{"list", CmdCategoryList},
	{"hash", CmdCategoryHash},
	{"string", CmdCategoryString},
	{"bitmap", CmdCategoryBitmap},
	{"hyperloglog", CmdCategoryHyperloglog},
	{"geo", CmdCategoryGeo},
	{"stream", CmdCategoryStream},
	{"pubsub", CmdCategoryPubSub},
	{"admin", CmdCategoryAdmin},
	{"fast", CmdCategoryFast},
	{"slow", CmdCategorySlow},
	{"blocking", CmdCategoryBlocking},
	{"dangerous", CmdCategoryDangerous},
	{"connection", CmdCategoryConnection},
	{"transaction", CmdCategoryTransaction},
	{"scripting", CmdCategoryScripting},
}

func ACLGetCommandCategoryFlagByName(flag string) uint64 {
	for _, category := range ACLCommandCategories {
		if category.flag > 0 && category.name == flag {
			return category.flag
		}
	}
	return 0
}

// clientReplyBlock.size = cap(buf)
type clientReplyBlock struct {
	//size int
	used int
	buf  []byte
}

const (
	CmdCallNone    = 0
	CmdCallSlowLog = 1 << iota
	CmdCallStats
	CmdCallPropacateAof
	CmdCallPropacateRepl
	CmdCallNowRap
	CmdCallPropacate = CmdCallPropacateAof | CmdCallPropacateRepl
	CmdCallFull      = CmdCallSlowLog | CmdCallStats | CmdCallPropacate
)

func processCommand(c *Client) error {
	moduleCallCommandFilters(c)

	if util.StrCmp((*sds.SDS)(c.argv[0].ptr).BufData(0), "quit") {
		addReply(c, shared.ok)
		c.flags |= CLIENT_CLOSE_AFTER_REPLY
		return C_ERR
	}

	c.cmd = lookupCommand(c.argv[0].ptr)
	c.lastCmd = c.cmd
	if c.cmd == nil {
		ss := make([]sds.SDS, len(c.argv)-1)
		for i := 1; i < c.argc; i++ {
			ss[i-1] = *(*sds.SDS)(c.argv[i].ptr)
		}
		args := sds.CatPrintf(ss...)
		rejectCommandFormat(c, "unknown command `%s`, with args beginning with: %s",
			(*sds.SDS)(c.argv[0].ptr).BufData(0), args)
		return C_OK
	} else if (c.cmd.arity > 0 && c.cmd.arity != c.argc) || c.argc < -c.cmd.arity {
		rejectCommandFormat(c, "wrong number of arguments for '%s' command",
			c.cmd.name)
		return C_OK
	}

	//isWriteCommand := c.cmd.flags&CmdWrite > 0 ||
	//	(c.cmd.name == "exec" && c.mstate.cmdFlags&CmdWrite > 0)
	//isDenyOOMCommand := c.cmd.flags&CmdDenyOom > 0 ||
	//	(c.cmd.name == "exec" && c.mstate.cmdFlags&CmdDenyOom > 0)
	//isDenyStaleCommand := c.cmd.flags&CmdStale > 0 ||
	//	(c.cmd.name == "exec" && c.mstate.cmdFlags&CmdStale > 0)
	//isDenyLoadingCommand := c.cmd.flags&CmdLoading > 0 ||
	//	(c.cmd.name == "exec" && c.mstate.cmdFlags&CmdLoading > 0)

	// todo other
	if authRequired(c) && c.flags&CmdNoAuth == 0 {
		rejectCommand(c, shared.noAuthErr)
		return C_OK
	}

	/**
	忽略了很多....todo todo
	*/

	call(c, CmdCallFull)
	return C_OK
}

func call(c *Client, flags int) {
	realCmd := c.cmd

	start := server.ustime
	c.cmd.proc(c)
	duration := time.Now().UnixMicro() - start
	if flags&CmdCallStats > 0 {
		realCmd.calls++
		realCmd.microseconds += uint64(duration)
	}
}

func rejectCommand(c *Client, reply *robj) {
	flagTransaction(c)

	if c.cmd != nil && c.cmd.name == "exec" {
		execCommandAbort(c, (*sds.SDS)(reply.ptr).BufData(0))
	} else {
		addReplyErrorObject(c, reply)
	}
}

func addReplyErrorObject(c *Client, reply *robj) {
	addReply(c, reply)
	afterErrorReply(c, (*sds.SDS)(reply.ptr).BufData(0))
}

func rejectCommandFormat(c *Client, format string, vv ...interface{}) {
	flagTransaction(c)

	err := fmt.Sprintf(format, vv...)
	err = strings.ReplaceAll(err, "\r\n", "  ")

	if c.cmd != nil && c.cmd.name == "exec" {
		execCommandAbort(c, err)
	} else {
		addReplyError(c, err)
	}
}

func flagTransaction(c *Client) {
	if c.flags&CLIENT_MULTI > 0 {
		c.flags |= CLIENT_DIRTY_EXEC
	}
}

func lookupCommand(key unsafe.Pointer) *redisCommand {
	return (*redisCommand)(server.commands.FetchValue(key))
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
	shared.pong = createObject(ObjString, sds.NewLen("+PONG\r\n"))
	shared.queue = createObject(ObjString, sds.NewLen("+QUEUED\r\n"))
	shared.emptyScan = createObject(ObjString, sds.NewLen("*2\r\n$1\r\n0\r\n*0\r\n"))
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

	/* The shared nil depends on the protocol version. */
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
		shared.mBulkHdr[j] = createObject(ObjString, sds.NewLen(fmt.Sprintf("*%d\r\n", j)))
		shared.bulkHdr[j] = createObject(ObjString, sds.NewLen(fmt.Sprintf("$%d\r\n", j)))
	}
	/* The following two shared objects, minstring and maxstrings, are not
	 * actually used for their value but as a special object meaning
	 * respectively the minimum possible string and the maximum possible
	 * string in string comparisons for the ZRANGEBYLEX command. */
	shared.minString = sds.NewLen("minstring")
	shared.maxString = sds.NewLen("maxstring")
}

var commandTableDictType = &dict.Type{
	HashFunction:  dictSdsCaseHash,
	KeyDup:        nil,
	ValDup:        nil,
	KeyCompare:    dictSdsKeyCaseCompare,
	KeyDestructor: dictSdsDestructor,
	ValDestructor: nil,
}
var dbDictType = &dict.Type{
	HashFunction:  dictSdsHash,
	KeyDup:        nil,
	ValDup:        nil,
	KeyCompare:    dictSdsKeyCompare,
	KeyDestructor: dictSdsDestructor,
	ValDestructor: dictObjectDestructor,
}
var keyPtrDictType = &dict.Type{
	HashFunction:  dictSdsHash,
	KeyDup:        nil,
	ValDup:        nil,
	KeyCompare:    dictSdsKeyCompare,
	KeyDestructor: nil,
	ValDestructor: nil,
}
var keyListDictType = &dict.Type{
	HashFunction:  dictObjHash,
	KeyDup:        nil,
	ValDup:        nil,
	KeyCompare:    dictObjKeyCompare,
	KeyDestructor: dictObjectDestructor,
	ValDestructor: dictListDestructor,
}
var objectKeyPointValueDictType = &dict.Type{
	HashFunction:  dictEncObjHash,
	KeyDup:        nil,
	ValDup:        nil,
	KeyCompare:    dictEncObjKeyCompare,
	KeyDestructor: dictObjectDestructor,
	ValDestructor: nil,
}

func dictSdsCaseHash(key unsafe.Pointer) uint64 {
	return dict.GenCaseHashFunction((*sds.SDS)(key).BufData(0))
}
func dictSdsHash(key unsafe.Pointer) uint64 {
	return dict.GenHashFunction((*sds.SDS)(key).BufData(0))
}
func dictSdsKeyCaseCompare(privData interface{}, key1, key2 unsafe.Pointer) bool {
	return util.BytesCaseCmp((*sds.SDS)(key1).BufData(0), (*sds.SDS)(key2).BufData(0))
}
func dictSdsKeyCompare(privData interface{}, key1, key2 unsafe.Pointer) bool {
	return util.BytesCmp((*sds.SDS)(key1).BufData(0), (*sds.SDS)(key2).BufData(0))
}
func dictSdsDestructor(privData interface{}, key unsafe.Pointer) {
}
func dictObjectDestructor(privData interface{}, value unsafe.Pointer) {
	if value == nil {
		return
	}
	(*robj)(value).decrRefCount()
}
func dictObjHash(key unsafe.Pointer) uint64 {
	return dict.GenHashFunction((*sds.SDS)((*robj)(key).ptr).BufData(0))
}
func dictObjKeyCompare(privData interface{}, _key1, _key2 unsafe.Pointer) bool {
	key1 := (*sds.SDS)((*robj)(_key1).ptr)
	key2 := (*sds.SDS)((*robj)(_key2).ptr)
	return util.BytesCmp(key1.BufData(0), key2.BufData(0))
}
func dictListDestructor(privData interface{}, val unsafe.Pointer) {
	(*adlist.List)(val).Release()
}
func dictEncObjHash(key unsafe.Pointer) uint64 {
	o := (*robj)(key)
	if o.sdsEncodedObject() {
		return dict.GenHashFunction((*sds.SDS)(o.ptr).BufData(0))
	} else if o.getEncoding() == ObjEncodingInt {
		return dict.GenHashFunction(util.String2Bytes(fmt.Sprintf("%v", *(*int)(o.ptr))))
	} else {
		o = o.getDecodedObject()
		hash := dict.GenHashFunction((*sds.SDS)(o.ptr).BufData(0))
		o.decrRefCount()
		return hash
	}
}
func dictEncObjKeyCompare(privData interface{}, key1, key2 unsafe.Pointer) bool {
	o1, o2 := (*robj)(key1), (*robj)(key1)
	if o1.getEncoding() == ObjEncodingInt && o2.getEncoding() == ObjEncodingInt {
		return *(*int)(o1.ptr) == *(*int)(o2.ptr)
	}

	if o1.refCount != ObjStaticRefCount {
		o1 = o1.getDecodedObject()
	}
	if o2.refCount != ObjStaticRefCount {
		o2 = o2.getDecodedObject()
	}
	cmp := util.BytesCmp((*sds.SDS)(o1.ptr).BufData(0), (*sds.SDS)(o2.ptr).BufData(0))
	if o1.refCount != ObjStaticRefCount {
		o1.decrRefCount()
	}
	if o2.refCount != ObjStaticRefCount {
		o2.decrRefCount()
	}
	return cmp
}

func beforeSleep(eventLoop *ae.EventLoop) {
	handleClientsWithPendingWrites()
}

func hasActiveChildProcess() bool {
	return server.rdbChildPid != -1 || server.aofChildPid != -1 || server.moduleChildPid != -1
}

func mstime() int64 {
	return time.Now().UnixMilli()
}
func ustime() int64 {
	return time.Now().UnixMicro()
}
