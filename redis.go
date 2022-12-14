package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var (
	supportOS = [5]string{"linux", "freebsd", "solaris", "windows", "darwin"}
)

func main() {
	checkOSSupport()

	redisServer := New()

	redisServer.Init()

	go func() {
		// todo graceful start/stop
		redisServer.Start()
		redisServer.Stop()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
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
