package main

import "time"

func getMonotonicUs() int64 {
	return time.Now().UnixNano() / 1e3
}

type TimeVal struct {
	Duration time.Duration
}
