package main

import "unsafe"

type readyList struct {
	db  *redisDb
	key *robj
}

func signalKeyAsReady(db *redisDb, key *robj) {
	if db.blockingKeys.FetchValue(unsafe.Pointer(key)) == nil {
		return
	}

	if db.readyKeys.FetchValue(unsafe.Pointer(key)) != nil {
		return
	}

	rl := &readyList{
		db:  db,
		key: key,
	}
	key.incrRefCount()
	server.readyKeys.AddNodeTail(rl)

	key.incrRefCount()
	db.readyKeys.Add(unsafe.Pointer(key), nil)
}
