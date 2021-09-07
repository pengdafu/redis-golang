package main

func main() {
	redisServer := New()

	redisServer.Init()
	redisServer.Start()
}
