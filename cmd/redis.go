package main

import (
	ae2 "github.com/pengdafu/redis-golang/pkg/ae"
	"log"
)

func main() {
	ae, err := ae2.CreateEventLoop(1024)
	if err != nil {
		log.Fatalln(err)
	}
	ae.Main()
}
