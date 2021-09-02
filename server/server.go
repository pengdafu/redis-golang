package server

import (
	"github.com/pengdafu/redis-golang/logic"
	"github.com/pengdafu/redis-golang/pkg/ae"
	"log"
	"os"
)

const (
	CONFIG_MIN_RESERVED_FDS = 32
	CONFIG_FDSET_INCR       = CONFIG_MIN_RESERVED_FDS + 96
)

type RedisServer struct {
	el         *ae.EventLoop
	maxclients int
}

func New() (redisServer *RedisServer) {
	redisServer = new(RedisServer)
	return
}

func (server *RedisServer) Init() {
	var err error
	server.el, err = ae.CreateEventLoop(server.maxclients + CONFIG_FDSET_INCR)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	if err := server.el.CreateTimeEvent(1, server.serverCron, nil, nil); err == ae.ERR {
		panic("Can't create event loop timer.")
	}


}

func (server *RedisServer) Start() error {
	logic.AeMain(server.el)
	return nil
}

func (server *RedisServer) serverCron(el *ae.EventLoop, id uint64, clientData interface{}) int {
	return 0
}
