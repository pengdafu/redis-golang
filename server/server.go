package server

import (
	"fmt"
	"github.com/pengdafu/redis-golang/logic"
	"github.com/pengdafu/redis-golang/pkg/ae"
	"github.com/pengdafu/redis-golang/pkg/net"
	"log"
	"os"
	"strings"
)

const (
	CONFIG_MIN_RESERVED_FDS = 32
	CONFIG_FDSET_INCR       = CONFIG_MIN_RESERVED_FDS + 96
	CONFIG_BINDADDR_MAX     = 16
)

var (
	C_OK error = nil
)

type RedisServer struct {
	el *ae.AeEventLoop

	port          int
	ipfd          *socketFds
	tcpBacklog    int
	bindAddr      [CONFIG_BINDADDR_MAX]string
	bindAddrCount int

	maxclients int
}

type socketFds struct {
	fd    [CONFIG_BINDADDR_MAX]int
	count int
}

func New() (redisServer *RedisServer) {
	redisServer = new(RedisServer)
	return
}

func (server *RedisServer) Init() {
	var err error

	server.ipfd = new(socketFds)
	server.port = 6379
	server.bindAddr[0] = "127.0.0.1"
	server.bindAddrCount = 1

	// 创建aeEventLoop
	server.el, err = ae.AeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR)
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
	if err := server.el.AeCreateTimeEvent(1, server.serverCron, nil, nil); err == ae.ERR {
		panic("Can't create event loop timer.")
	}

	// 创建连接处理
	if server.createSocketAcceptHandler(server.ipfd, logic.AcceptTcpHandler) != C_OK {
		panic("Unrecoverable error creating TCP socket accept handler.")
	}
}

func (server *RedisServer) Start() error {
	logic.AeMain(server.el)
	return nil
}

func (server *RedisServer) serverCron(el *ae.AeEventLoop, id uint64, clientData interface{}) int {
	return 0
}

func (server *RedisServer) createSocketAcceptHandler(sfd *socketFds, accessHandle ae.AeFileProc) error {
	for i := 0; i < sfd.count; i++ {
		if err := server.el.AeCreateFileEvent(sfd.fd[i], ae.READABLE, accessHandle, nil); err != nil {
			for j := i - 1; j >= 0; j-- {
				server.el.AeDeleteFileEvent(sfd.fd[j], ae.READABLE)
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
		if strings.Index(addr, ":") != -1 {
			sfd.fd[sfd.count], err = net.AnetTcp6Server(port, addr, server.tcpBacklog)
		} else {
			sfd.fd[sfd.count], err = net.AnetTcpServer(port, addr, server.tcpBacklog)
		}
		if err != nil {
			server.closeSocketListeners(sfd)
			return err
		}
		_ = net.AnetNonBlock(sfd.fd[sfd.count])
		_ = net.AnetCloexec(sfd.fd[sfd.count])
		sfd.count++
	}
	return nil
}

func (server *RedisServer) closeSocketListeners(sfd *socketFds) {
	for i := 0; i < sfd.count; i++ {
		if sfd.fd[i] == -1 {
			continue
		}
		server.el.AeDeleteFileEvent(sfd.fd[i], ae.READABLE)
	}
	sfd.count = 0
}
