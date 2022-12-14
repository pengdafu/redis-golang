package main

import "time"

const (
	LfuInitVal = 5
)
const (
	LruBits            = 24
	LruClockMax        = 1<<LruBits - 1
	LruClockResolution = 1000
)

func LFUGetTimeInMinutes() uint16 {
	return uint16((server.unixtime / 60) & 65535)
}

func LRU_CLOCK() uint32 {
	var lruClock uint32
	if 1000/server.hz <= LruClockResolution {
		lruClock = server.lruClock
	} else {
		lruClock = getLRUClock()
	}
	return lruClock
}

func getLRUClock() uint32 {
	return uint32((time.Now().UnixMilli() / LruClockResolution) & LruClockMax)
}
