package main

import (
	"github.com/pengdafu/redis-golang/sds"
	"github.com/pengdafu/redis-golang/util"
)

func getCommand(c *Client) {
	getGenericCommand(c)
}

func getGenericCommand(c *Client) {
	var o *robj
	if o = lookupKeyReadOrReply(c, c.argv[1], shared.null[c.resp]); o == nil {
		return
	}
	if o.getType() != ObjString {
		addReply(c, shared.wrongTypeErr)
		return
	}
	addReplyBulk(c, o)
}

const (
	objSetNX = 1 << iota
	objSetXX
	objSetEX
	objSetPX
	objSetKeepTTL
	objSetNoFlags = 0
)

func setCommand(c *Client) {
	flags := objSetNoFlags
	var expire *robj
	unit := unitSeconds
	for i := 3; i < c.argc; i++ {
		a := (*sds.SDS)(c.argv[i].ptr)
		var next *robj
		if i != c.argc-1 {
			next = c.argv[i+1]
		}

		buf := a.BufData(0)
		if util.BytesCaseCmp(buf, []byte{'n', 'x'}) && flags&objSetXX == 0 {
			flags |= objSetNX
		} else if util.BytesCaseCmp(buf, []byte{'x', 'x'}) && flags&objSetNX == 0 {
			flags |= objSetXX
		} else if util.StrCaseCmp(buf, "keepttl") && flags&objSetEX == 0 && flags&objSetPX == 0 {
			flags |= objSetKeepTTL
		} else if util.BytesCaseCmp(buf, []byte{'e', 'x'}) && flags&objSetKeepTTL == 0 && flags&objSetPX == 0 && next != nil {
			flags |= objSetEX
			expire = next
			i++
			unit = unitSeconds
		} else if util.BytesCaseCmp(buf, []byte{'p', 'x'}) && flags&objSetKeepTTL == 0 && flags&objSetEX == 0 && next != nil {
			flags |= objSetPX
			expire = next
			i++
			unit = unitMilliSeconds
		} else {
			addReply(c, shared.syntaxErr)
			return
		}
	}

	c.argv[2] = c.argv[2].tryObjectEncoding()
	setGenericCommand(c, flags, c.argv[1], c.argv[2], expire, unit, nil, nil)
}

func setGenericCommand(c *Client, flags int, key, val, expire *robj, unit int, okReply, abortReply *robj) {
	milliseconds := int64(0)
	if expire != nil {
		if err := expire.getLongLongFromObjectOrReply(c, &milliseconds, ""); err != C_OK {
			return
		}
		if milliseconds <= 0 {
			addReplyErrorFormat(c, "invalid expire time in %s", c.cmd.name)
			return
		}
		if unit == unitSeconds {
			milliseconds *= 1000
		}
	}

	if (flags&objSetNX > 0 && c.db.lookupKeyWrite(key) != nil) ||
		(flags&objSetXX > 0 && c.db.lookupKeyWrite(key) == nil) {
		reply := abortReply
		if reply == nil {
			reply = shared.null[c.resp]
		}
		addReply(c, reply)
		return
	}

	c.db.genericSetKey(c, key, val, flags&objSetKeepTTL > 0, true)
	server.dirty++
	if expire != nil {
		c.db.setExpire(c, key, mstime()+milliseconds)
	}
	notifyKeySpaceEvent(notifyString, "set", key, c.db.id)
	if expire != nil {
		notifyKeySpaceEvent(notifyGeneric, "expire", key, c.db.id)
	}
	reply := okReply
	if reply == nil {
		reply = shared.ok
	}
	addReply(c, reply)
}

func delCommand(c *Client) {
	delGenericCommand(c, server.lazyFreeLazyUserDel)
}
func unlinkCommand(c *Client) {
	delGenericCommand(c, true)
}

func delGenericCommand(c *Client, lazy bool) {
	var numDel int
	for i := 1; i < c.argc; i++ {
		c.db.expireIfNeeded(c.argv[i])
		deleteFn := dbASyncDelete
		if !lazy {
			deleteFn = dbSyncDelete
		}
		deleted := deleteFn(c.db, c.argv[i])
		if deleted {
			signalModifiedKey(c, c.db, c.argv[i])
			notifyKeySpaceEvent(notifyGeneric, "del", c.argv[i], c.db.id)
			server.dirty++
			numDel++
		}
	}
	addReplyLongLong(c, numDel)
}
