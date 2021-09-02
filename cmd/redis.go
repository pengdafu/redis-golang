package main

import (
	"github.com/pengdafu/redis-golang/server"
)

func main() {
	redisServer := server.New()

	redisServer.Init()
	redisServer.Start()
}
