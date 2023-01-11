package main

import (
	"github.com/pengdafu/redis-golang/dict"
	"github.com/pengdafu/redis-golang/sds"
)

const (
	activeExpireCycleSlow = iota
	activeExpireCycleFast
)
const (
	activeExpireCycleKeysPerLoop     = 20
	activeExpireCycleFastDuration    = 1000 // microseconds
	activeExpireCycleSlowTimePerc    = 25   // 最多使用多少CPU
	activeExpireCycleAcceptableStale = 10
)

func activeExpireCycle(typ int) {
	effort := server.activeExpireEffort - 1
	configKeysPerLoop := int64(activeExpireCycleKeysPerLoop + activeExpireCycleKeysPerLoop/4*effort)
	configCycleFastDuration := int64(activeExpireCycleFastDuration + activeExpireCycleFastDuration/4*effort)
	configCycleSlowTimePerc := int64(activeExpireCycleSlowTimePerc + 2*effort)
	configCycleAcceptableStale := activeExpireCycleAcceptableStale - effort

	currentDb := 0
	timeLimitExit := false
	lastFastCycle := int64(0)

	dbsPerCall := CRON_DBS_PER_CALL
	start := ustime()
	var timeLimit, elapsed int64
	if clientsArePaused() {
		return
	}

	if typ == activeExpireCycleFast {
		if !timeLimitExit && server.statExpiredStalePerc < configCycleAcceptableStale {
			return
		}
		if start < lastFastCycle+configCycleFastDuration*2 {
			return
		}
		lastFastCycle = start
	}

	if dbsPerCall > server.dbnum || timeLimitExit {
		dbsPerCall = server.dbnum
	}

	timeLimit = configCycleSlowTimePerc * 1000000 / int64(server.hz) / 100
	timeLimitExit = false
	if timeLimit <= 0 {
		timeLimit = 1
	}

	if typ == activeExpireCycleFast {
		timeLimit = configCycleFastDuration
	}

	iteration := 0

	var totalSampled, totalExpired int64
	for j := 0; j < dbsPerCall && !timeLimitExit; j++ {
		var expired, sampled int64
		db := server.db[currentDb%server.dbnum]
		currentDb++
		for sampled == 0 || expired*100/sampled > int64(configCycleAcceptableStale) {
			iteration++
			var num, slots int64
			if num = db.expires.Size(); num == 0 {
				db.avgTTL = 0
				break
			}
			slots = db.expires.Slots()
			now := mstime()
			if now > 0 && slots > dict.HtInitialSize && num*100/slots < 1 {
				break
			}

			expired = 0
			sampled = 0
			ttlSum := int64(0)
			ttlSamples := int64(0)
			if num > configKeysPerLoop {
				num = configKeysPerLoop
			}

			maxBuckets := num * 20
			checkedBuckets := int64(0)
			for sampled < num && checkedBuckets < maxBuckets {
				for table := 0; table < 2; table++ {
					if table == 1 && !db.expires.IsRehashing() {
						break
					}

					idx := db.expiresCursor
					idx &= db.expires.SizeMask(table)
					de := db.expires.DictEntry(table, idx)

					var ttl int64
					checkedBuckets++
					for de != nil {
						e := de
						de = de.Next()
						ttl = dict.GetSignedIntegerVal(e) - now
						if activeExpireCycleTryExpire(db, e, now) {
							expired++
						}
						if ttl > 0 {
							ttlSum += ttl
							ttlSamples++
						}
						sampled++
					}
				}
				db.expiresCursor++
			}
			totalExpired += expired
			totalSampled += sampled
			if ttlSamples > 0 {
				avgTtl := ttlSum / ttlSamples
				if db.avgTTL == 0 {
					db.avgTTL = avgTtl
				}
				db.avgTTL = (db.avgTTL/50)*49 + avgTtl/50
			}

			if iteration&0xf == 0 {
				elapsed = ustime() - start
				if elapsed > timeLimit {
					timeLimitExit = true
					server.statExpiredTimeCapReachedCount++
					break
				}
			}
		}
	}
}

func expireSlaveKeys() {

}

func activeExpireCycleTryExpire(db *redisDb, de *dict.Entry, now int64) bool {
	t := dict.GetSignedIntegerVal(de)
	if now > t {
		key := (*sds.SDS)(dict.GetKey(de))
		keyObj := createObject(ObjString, *key)
		db.propagateExpire(keyObj, server.lazyFreeLazyExpire)
		if server.lazyFreeLazyExpire {
			dbASyncDelete(db, keyObj)
		} else {
			dbSyncDelete(db, keyObj)
		}
		notifyKeySpaceEvent(notifyExpired, "expired", keyObj, db.id)
		signalModifiedKey(nil, db, keyObj)
		keyObj.decrRefCount()
		server.statExpiredKeys++
		return true
	}
	return false
}

func expireCommand(c *Client) {
	expireGenericCommand(c, mstime(), unitSeconds)
}

func expireGenericCommand(c *Client, basetime int64, unit int) {
	key := c.argv[1]
	param := c.argv[2]

	var when int64
	if err := param.getLongLongFromObjectOrReply(c, &when, ""); err != C_OK {
		return
	}
	if unit == unitSeconds {
		when *= 1000
	}
	when += basetime
	if c.db.lookupKeyWrite(key) == nil {
		addReply(c, shared.czero)
		return
	}

	if checkAlreadyExpired(when) {
		var aux *robj
		deleteFn := dbASyncDelete
		if !server.lazyFreeLazyExpire {
			deleteFn = dbSyncDelete
		}
		deleted := deleteFn(c.db, key)
		if !deleted {
			panic("delete err")
		}

		server.dirty++

		aux = shared.del
		if server.lazyFreeLazyExpire {
			aux = shared.unlink
		}
		rewriteClientCommandVector(c, 2, aux, key)
		signalModifiedKey(c, c.db, key)
		notifyKeySpaceEvent(notifyGeneric, "del", key, c.db.id)
		addReply(c, shared.cone)
	} else {
		c.db.setExpire(c, key, when)
		addReply(c, shared.cone)
		signalModifiedKey(c, c.db, key)
		notifyKeySpaceEvent(notifyGeneric, "expire", key, c.db.id)
		server.dirty++
	}
}

func checkAlreadyExpired(when int64) bool {
	return when <= mstime() && !server.loading && server.masterhost == ""
}
