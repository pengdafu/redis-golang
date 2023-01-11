package main

import (
	"github.com/pengdafu/redis-golang/sds"
	"log"
)

const (
	notifyKeySpace = 1 << iota
	notifyKeyEvent
	notifyGeneric
	notifyString
	notifyList
	notifySet
	notifyHash
	notifyZset
	notifyExpired
	notifyEvicted
	notifyStream
	notifyKeyMiss
	notifyLoaded
	notifyAll = notifyGeneric | notifyString | notifyList | notifySet | notifyHash | notifyZset | notifyExpired | notifyEvicted | notifyStream
)

// todo
func notifyKeySpaceEvent(typ int, event string, key *robj, dbId int) {
	log.Printf("type:%d, event: %s, key: %s, dbId: %d\n", typ, event, (*sds.SDS)(key.ptr).BufData(0), dbId)
}
