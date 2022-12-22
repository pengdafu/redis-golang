package main

import (
	"github.com/pengdafu/redis-golang/adlist"
	"github.com/pengdafu/redis-golang/dict"
	"github.com/pengdafu/redis-golang/sds"
	"unsafe"
)

type redisDb struct {
	dict          *dict.Dict
	expires       *dict.Dict
	blockingKeys  *dict.Dict
	readyKeys     *dict.Dict
	watchedKeys   *dict.Dict
	id            int
	avgTTL        int64
	expiresCursor uint64
	defragLater   *adlist.List
}

func selectDb(c *Client, id int) error {
	if id < 0 || id >= server.dbnum {
		return C_ERR
	}

	c.db = server.db[id]
	return C_OK
}

const (
	lookupNone     = 0
	lookupNoTouch  = 1 << 0
	lookupNoNotify = 1 << 1
)

func (db *redisDb) lookupKeyWrite(key *robj) *robj {
	return db.lookupKeyWriteWithFlags(key, lookupNone)
}

func (db *redisDb) lookupKeyWriteWithFlags(key *robj, flags int) *robj {
	db.expireIfNeeded(key)
	return db.lookupKey(key, flags)
}

func (db *redisDb) lookupKey(key *robj, flags int) *robj {
	de := db.dict.FetchValue(key.ptr)
	if de != nil {
		val := (*robj)(de)

		if !hasActiveChildProcess() && flags&lookupNoTouch == 0 {
			if server.maxMemoryPolicy&MaxMemoryFlagLfu > 0 {
				updateLFU(val)
			} else {
				val.setLru(LRU_CLOCK())
			}
		}

		return val
	} else {
		return nil
	}
}

func (db *redisDb) expireIfNeeded(key *robj) bool {
	// todo expire
	return false
}

func updateLFU(val *robj) {
	// todo
}

func (db *redisDb) genericSetKey(c *Client, key, val *robj, keepTtl, signal bool) {
	if db.lookupKeyWrite(key) == nil {
		db.dbAdd(key, val)
	} else {
		db.dbOverwrite(key, val)
	}
	val.incrRefCount()
	if !keepTtl {
		db.removeExpire(key)
	}
	if signal {
		db.signalModifiedKey(c, key)
	}
}

func (db *redisDb) dbOverwrite(key, val *robj) {
	de := db.dict.Find(key.ptr)
	old := (*robj)(db.dict.GetVal(de))
	if server.maxMemoryPolicy&MaxMemoryFlagLfu > 0 {
		val.setLru(old.getLru())
	}

	db.dict.SetVal(de, unsafe.Pointer(val))
}

func (db *redisDb) removeExpire(key *robj) {
	db.expires.Delete(key.ptr)
}

// todo
func (db *redisDb) signalModifiedKey(c *Client, key *robj) {

}

func (db *redisDb) dbAdd(key, val *robj) {
	dup := sds.Dup(*(*sds.SDS)(key.ptr))
	db.dict.Add(unsafe.Pointer(&dup), unsafe.Pointer(val))

	if val.getType() == ObjList ||
		val.getType() == ObjZSet ||
		val.getType() == ObjStream {
		signalKeyAsReady(db, key)
	}

	if server.clusterEnabled {
		slotToKeyAdd((*sds.SDS)(key.ptr))
	}
}

// todo
func slotToKeyAdd(key *sds.SDS) {

}

func (db *redisDb) setExpire(c *Client, key *robj, when int64) {
	kde := db.dict.Find(key.ptr)
	if kde == nil {
		panic("kde should not be nil")
	}

	de := db.expires.AddOrFind(key.ptr)
	db.expires.SetSignedInterVal(de, when)

	//writableSlave := server.masterHost != "" && server.replSlaveRo == 0
	//if c != nil && writableSlave && c.flags&CLIENT_MASTER == 0 {
	//	db.rememberSlaveKeyWithExpire(key)
	//}
}
