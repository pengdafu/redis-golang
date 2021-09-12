package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

const (
	CONFIG_MIN_RESERVED_FDS = 32
	CONFIG_FDSET_INCR       = CONFIG_MIN_RESERVED_FDS + 96
	CONFIG_BINDADDR_MAX     = 16
	CONFIG_RUN_ID_SIZE      = 40
)

var (
	C_OK  error = nil
	C_ERR error = errors.New("error")
)

type socketFds struct {
	fd    [CONFIG_BINDADDR_MAX]int
	count int
}
type RedisServer struct {
	el *AeEventLoop

	port          int
	ipfd          *socketFds
	tcpBacklog    int
	bindAddr      [CONFIG_BINDADDR_MAX]string
	bindAddrCount int

	clients          []interface{}
	clusterEnabled   bool
	statRejectedConn uint64
	tcpKeepalive     int
	maxclients       int
}

var server *RedisServer

type Client struct {
	id              uint64 // 自增唯一ID
	conn            *Connection
	resp            int // resp 协议版本，可以是2或者3
	db              []*redisDb
	name            *robj // 客户端名字，通过SETNAME设置
	querybuf        sds   // 缓存客户端请求的buf
	qbPos           int   // querybuf 读到的位置
	pendingQueryBuf sds   // 如果此客户端被标记为主服务器，则此缓冲区表示从主服务器接收的复制流中尚未应用的部分。

	querybufPeak int           // 最近100ms或者更长时间querybuf的峰值
	argc         int           // 当前command有多少个参数
	argv         []*robj       // 当前command的参数列表
	originalArgc int           // 如果参数被重写，原始参数的个数
	originalArgv []*robj       // 如果参数被重写，原始的参数列表
	argvLenSum   int           // len(argv)
	cmd, lastCmd *redisCommand // 最后一次执行的command
	user         *user         // user与connect关联，如果为nil，则代表是admin，可以做任何操作

	reqType                  int // request protocol type: PROTO_REQ_*
	multiBulkLen             int // 要读取的多个批量参数的数量
	bulkLen                  int // 批量请求的参数长度
	reply                    *list
	replyBytes               uint64        // 要响应的字节长度
	sentLen                  uint64        // 当前缓冲区或者正在发送中的对象已经发送的字节数
	ctime                    time.Duration // 客户端创建时间
	duration                 int64         // 当前command的运行时间，用来阻塞或非阻塞命令的延迟
	lastInteraction          time.Duration // 上次交互时间，用于超时
	obufSoftLimitReachedTime time.Duration
	flags                    int                          // 客户端的flag，CLIENT_* 宏定义
	authenticated            bool                         // 当默认用户需要认证
	replState                int                          // 如果client是一个从节点，则为从节点的复制状态
	replPutOnlineOnAck       int                          // 在第一个ACK的时候，安装从节点的写处理器
	replDBFd                 int                          // 复制database 的文件描述符
	replDBOff                int                          // 复制database的文件的偏移
	replDBSize               int                          // 复制db的文件的大小
	replPreamble             int                          // 复制DB序言
	readReplOff              int                          // Read replication offset if this is a master
	replOff                  int                          // Applied replication offset if this is a master
	replAckOff               int                          // Replication ack offset, if this is a slave
	replAckTime              int                          // Replication ack time, if this is a slave
	psyncInitialOffset       int                          // FULLRESYNC reply offset other slaves copying this slave output buffer should use.
	replId                   [CONFIG_RUN_ID_SIZE + 1]byte // 主复制Id，如果是主节点
	slaveListeningPort       int                          // As configured with: REPLCONF listening-port
	slaveAddr                string                       // Optionally given by REPLCONF ip-address
	slaveCapa                int                          // 从节点容量：SLAVE_CAPA_* bitwise OR
	mstate                   multiState                   // MULTI/EXEC state
	bType                    int                          // 如果是CLIENT_BLOCKED类型，表示阻塞
	bpop                     blockingState                // blocking state
	woff                     int                          // 最后一次写的全局复制偏移量
	watchedKeys              *list                        // Keys WATCHED for MULTI/EXEC CAS
	pubSubChannels           *dict                        // 客户端关注的渠道(SUBSCRIBE)
	pubSubPatterns           *list                        // 客户端关注的模式(SUBSCRIBE)
	peerId                   sds                          // Cached peer ID
	sockName                 sds                          // Cached connection target address.
}
type redisDb struct {
	// todo redisDb
}
type robj = redisObject
type redisObject struct {
	// todo redisObject

}
type redisCommand struct {
}
type user struct {
}
type multiState struct {
}
type blockingState struct {
}

func New() *RedisServer {
	server = new(RedisServer)
	return server
}

func (server *RedisServer) Init() {
	var err error

	server.ipfd = new(socketFds)
	server.port = 6379
	server.bindAddr[0] = "127.0.0.1"
	server.bindAddrCount = 1

	// 创建aeEventLoop
	server.el, err = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR)
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
	if err := server.el.aeCreateTimeEvent(1, server.serverCron, nil, nil); err == ERR {
		panic("Can't create event loop timer.")
	}

	// 创建连接处理
	if server.createSocketAcceptHandler(server.ipfd, acceptTcpHandler) != C_OK {
		panic("Unrecoverable error creating TCP socket accept handler.")
	}
}

func (server *RedisServer) Start() error {
	aeMain(server.el)
	return nil
}

func (server *RedisServer) serverCron(el *AeEventLoop, id uint64, clientData interface{}) int {
	return 0
}

func (server *RedisServer) createSocketAcceptHandler(sfd *socketFds, accessHandle AeFileProc) error {
	for i := 0; i < sfd.count; i++ {
		if err := server.el.aeCreateFileEvent(sfd.fd[i], AE_READABLE, accessHandle, nil); err != nil {
			for j := i - 1; j >= 0; j-- {
				server.el.aeDeleteFileEvent(sfd.fd[j], AE_READABLE)
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
			sfd.fd[sfd.count], err = anetTcp6Server(port, addr, server.tcpBacklog)
		} else {
			sfd.fd[sfd.count], err = anetTcpServer(port, addr, server.tcpBacklog)
		}
		if err != nil {
			server.closeSocketListeners(sfd)
			return err
		}
		_ = anetNonBlock(sfd.fd[sfd.count])
		_ = anetCloexec(sfd.fd[sfd.count])
		sfd.count++
	}
	return nil
}

func (server *RedisServer) closeSocketListeners(sfd *socketFds) {
	for i := 0; i < sfd.count; i++ {
		if sfd.fd[i] == -1 {
			continue
		}
		server.el.aeDeleteFileEvent(sfd.fd[i], AE_READABLE)
	}
	sfd.count = 0
}

func (server *RedisServer) connSocketClose(conn *Connection) {

}
