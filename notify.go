package main

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

}
