package main

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	supportOS = [5]string{"linux", "freebsd", "solaris", "windows", "darwin"}
)

func main() {
	checkOSSupport()

	rand.Seed(time.Now().UnixNano())

	redisServer := New()

	redisServer.InitServer()

	go func() {
		// todo graceful start/stop
		redisServer.Start()
	}()

	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-signals
	redisServer.Stop()
}

func checkOSSupport() {
	os := runtime.GOOS
	for _, sos := range supportOS {
		if os == sos {
			return
		}
	}
	log.Printf("unsupport os: %s", os)
}
