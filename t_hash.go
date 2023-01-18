package main

import (
	"fmt"
	"github.com/pengdafu/redis-golang/dict"
	"github.com/pengdafu/redis-golang/sds"
	"github.com/pengdafu/redis-golang/ziplist"
	"unsafe"
)

func hsetCommand(c *Client) {
	if c.argc%2 == 1 {
		addReplyErrorFormat(c, "wrong number of arguments for '%s' command", c.cmd.name)
		return
	}
	var o *robj
	if o = hashTypeLookupWriteOrCreate(c, c.argv[1]); o == nil {
		return
	}
	hashTypeTryConversion(o, c.argv, 2, c.argc-1)

	var created int
	for i := 2; i < c.argc; i += 2 {
		created += (hashTypeSet(o, *(*sds.SDS)(c.argv[i].ptr), *(*sds.SDS)(c.argv[i+1].ptr), hashSetCopy) + 1) % 2
	}

	cmdname := (*sds.SDS)(c.argv[0].ptr).BufData(0)
	if cmdname[1] == 's' || cmdname[1] == 'S' {
		addReplyLongLong(c, created)
	} else {
		addReply(c, shared.ok)
	}
	signalModifiedKey(c, c.db, c.argv[1])
	notifyKeySpaceEvent(notifyHash, "hset", c.argv[1], c.db.id)
	server.dirty++
}

const (
	hashSetCopy      = 0
	hashSetTakeFiled = 1 << 0
	hashSetTakeValue = 1 << 1
)

func hashTypeSet(o *robj, field, value sds.SDS, flags int) int {
	var update = 0

	if o.getEncoding() == ObjEncodingZipList {
		zl := *(*[]byte)(o.ptr)
		fptr := ziplist.Index(zl, ziplist.Head)
		var vptr []byte
		if fptr != nil {
			fptr = ziplist.Find(fptr, field.BufData(0), sds.Len(field), 1)
			if fptr != nil {
				vptr = ziplist.Next(zl, fptr)
				update = 1

				zl = ziplist.Delete(zl, &vptr)
				zl = ziplist.Insert(zl, vptr, value.BufData(0))
			}
		}

		if update == 0 {
			zl = ziplist.Push(zl, field.BufData(0), ziplist.Tail)
			zl = ziplist.Push(zl, value.BufData(0), ziplist.Tail)
		}
		o.ptr = unsafe.Pointer(&zl)

		if hashTypeLength(o) > server.hashMaxZipListEntries {
			hashTypeConvert(o, ObjEncodingHt)
		}
	} else if o.getEncoding() == ObjEncodingHt {
		d := (*dict.Dict)(o.ptr)
		de := d.Find(unsafe.Pointer(&field))
		if de != nil {
			if flags&hashSetTakeValue > 0 {
				dict.SetVal(de, unsafe.Pointer(&value))
			} else {
				dup := sds.Dup(value)
				dict.SetVal(de, unsafe.Pointer(&dup))
			}
			update = 1
		} else {
			var f, v sds.SDS
			if flags&hashSetTakeFiled > 0 {
				f = field
			} else {
				f = sds.Dup(field)
			}
			if flags&hashSetTakeValue > 0 {
				v = value
			} else {
				v = sds.Dup(value)
			}
			d.Add(unsafe.Pointer(&f), unsafe.Pointer(&v))
		}
	} else {
		panic("Unknown hash encoding")
	}

	if flags&hashSetTakeFiled > 0 {
		//sdsFree(field)
	}
	if flags&hashSetTakeValue > 0 {
		//sdsFree(value)
	}
	return update
}

func hgetCommand(c *Client) {
	var o *robj
	if o = lookupKeyReadOrReply(c, c.argv[1], shared.null[c.resp]); o == nil || o.checkType(c, ObjHash) {
		return
	}

	addHashFieldToReply(c, o, *(*sds.SDS)(c.argv[2].ptr))
}

func hmgetCommand(c *Client) {
	o := c.db.lookupKeyRead(c.argv[1])
	if o != nil && o.getType() != ObjHash {
		addReply(c, shared.wrongTypeErr)
		return
	}

	addReplyArrayLen(c, c.argc-2)
	for i := 2; i < c.argc; i++ {
		addHashFieldToReply(c, o, *(*sds.SDS)(c.argv[i].ptr))
	}
}

func hdelCommand(c *Client) {
	var o *robj
	if o = lookupKeyWriteOrReply(c, c.argv[1], shared.czero); o == nil || o.checkType(c, ObjHash) {
		return
	}

	var deleted int
	var keyRemoved bool
	for j := 2; j < c.argc; j++ {
		if hashTypeDelete(o, *(*sds.SDS)(c.argv[j].ptr)) {
			deleted++
		}
		if hashTypeLength(o) == 0 {
			dbDelete(c.db, c.argv[1])
			keyRemoved = true
			break
		}
	}

	if deleted > 0 {
		signalModifiedKey(c, c.db, c.argv[1])
		notifyKeySpaceEvent(notifyHash, "hdel", c.argv[1], c.db.id)
		if keyRemoved {
			notifyKeySpaceEvent(notifyGeneric, "del", c.argv[1], c.db.id)
		}
		server.dirty++
	}
	addReplyLongLong(c, deleted)
}

func hashTypeDelete(o *robj, field sds.SDS) bool {
	var deleted bool
	if o.getEncoding() == ObjEncodingZipList {
		zl := *(*[]byte)(o.ptr)
		fptr := ziplist.Index(zl, ziplist.Head)
		if fptr != nil {
			fptr = ziplist.Find(fptr, field.BufData(0), sds.Len(field), 1)
			if fptr != nil {
				zl = ziplist.Delete(zl, &fptr) // delete key
				zl = ziplist.Delete(zl, &fptr) // delete value
				o.ptr = unsafe.Pointer(&zl)
				deleted = true
			}
		}
	} else if o.getEncoding() == ObjEncodingHt {
		d := (*dict.Dict)(o.ptr)
		if d.Delete(unsafe.Pointer(&field)) {
			deleted = true
			if htNeedResize(d) {
				d.Resize()
			}
		}
	} else {
		panic("Unknown hash encoding")
	}
	return deleted
}

func hgetallCommand(c *Client) {
	genericHgetallCommand(c, objHashKey|objHashValue)
}

func hkeysCommand(c *Client) {
	genericHgetallCommand(c, objHashKey)
}

func hvalsCommand(c *Client) {
	genericHgetallCommand(c, objHashValue)
}

func addHashIteratorCursorToReply(c *Client, hi *hashTypeIterator, what int) {
	if hi.encoding == ObjEncodingZipList {
		var vstr []byte
		var vlen int
		var vll int64
		hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll)
		if vstr != nil {
			addReplyBulkBuffer(c, vstr, vlen)
		} else {
			addReplyBulkLongLong(c, vll)
		}
	} else if hi.encoding == ObjEncodingHt {
		value := hashTypeCurrentFromHashTable(hi, what)
		addReplyBulkBuffer(c, value.BufData(0), sds.Len(value))
	} else {
		panic("Unknown hash encoding")
	}
}

func genericHgetallCommand(c *Client, flags int) {
	var o *robj
	var hi *hashTypeIterator

	emptyResp := shared.emptyMap[c.resp]
	if !(flags&objHashKey > 0 && flags&objHashValue > 0) {
		emptyResp = shared.emptyArray
	}
	if o = lookupKeyReadOrReply(c, c.argv[1], emptyResp); o == nil || o.checkType(c, ObjHash) {
		return
	}

	length := hashTypeLength(o)
	if flags&objHashKey > 0 && flags&objHashValue > 0 {
		addReplyMapLen(c, length)
	} else {
		addReplyArrayLen(c, length)
	}

	var count int
	hi = hashTypeInitIterator(o)
	for hashTypeNext(hi) != C_ERR {
		if flags&objHashKey > 0 {
			addHashIteratorCursorToReply(c, hi, objHashKey)
			count++
		}
		if flags&objHashValue > 0 {
			addHashIteratorCursorToReply(c, hi, objHashValue)
			count++
		}
	}

	hashTypeReleaseIterator(hi)

	if flags&objHashKey > 0 && flags&objHashValue > 0 {
		count /= 2
	}
	if count != length {
		panic(fmt.Sprintf("genericHgetallCommand count != length(%d! = %d)", count, length))
	}
}

func addHashFieldToReply(c *Client, o *robj, field sds.SDS) {
	if o == nil {
		addReplyNull(c)
		return
	}

	if o.getEncoding() == ObjEncodingZipList {
		var vstr []byte
		var vlen int
		var vll int64
		if hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) < 0 {
			addReplyNull(c)
		} else {
			if vstr != nil {
				addReplyBulkBuffer(c, vstr, vlen)
			} else {
				addReplyBulkLongLong(c, vll)
			}
		}
	} else if o.getEncoding() == ObjEncodingHt {
		value := hashTypeGetFromHashTable(o, field)
		if value != nil {
			addReplyBulkBuffer(c, value.BufData(0), sds.Len(*value))
		} else {
			addReplyNull(c)
		}
	} else {
		panic("Unknown hash encoding")
	}
}

func hashTypeGetFromHashTable(o *robj, field sds.SDS) *sds.SDS {
	d := (*dict.Dict)(o.ptr)

	de := d.Find(unsafe.Pointer(&field))
	if de == nil {
		return nil
	}
	return (*sds.SDS)(dict.GetVal(de))
}

func hashTypeGetFromZiplist(o *robj, field sds.SDS, vstr *[]byte, vlen *int, vll *int64) (ret int) {
	zl := *(*[]byte)(o.ptr)
	fptr := ziplist.Index(zl, ziplist.Head)
	var vptr []byte
	if fptr != nil {
		fptr = ziplist.Find(fptr, field.BufData(0), sds.Len(field), 1)
		if fptr != nil {
			vptr = ziplist.Next(zl, fptr)
		}
	}

	if vptr != nil {
		if !ziplist.Get(vptr, vstr, vlen, vll) {
			return -1
		}
		return 0
	}
	return -1
}

func hashTypeLookupWriteOrCreate(c *Client, key *robj) *robj {
	o := c.db.lookupKeyWrite(key)
	if o == nil {
		o = createHashObject()
		c.db.dbAdd(key, o)
	} else {
		if o.getType() != ObjHash {
			addReply(c, shared.wrongTypeErr)
			return nil
		}
	}
	return o
}

func hashTypeTryConversion(o *robj, argv []*robj, start, end int) {
	sum := 0
	if o.getEncoding() != ObjEncodingZipList {
		return
	}
	for i := start; i <= end; i++ {
		if !argv[i].sdsEncodedObject() {
			continue
		}
		l := sds.Len(*(*sds.SDS)(o.ptr))
		if l > server.hashMaxZipListValue {
			hashTypeConvert(o, ObjEncodingHt)
			return
		}
		sum += l
	}

	if !ziplist.SafeToAdd(*(*[]byte)(o.ptr), sum) {
		hashTypeConvert(o, ObjEncodingHt)
	}
}

func hashTypeConvert(o *robj, enc uint32) {
	if o.getEncoding() == ObjEncodingZipList {
		hashTypeConvertZiplist(o, enc)
	} else if o.getEncoding() == ObjEncodingHt {
		panic("Not implemented")
	} else {
		panic("Unknown hash encoding")
	}
}

type hashTypeIterator struct {
	subject    *robj
	encoding   int
	fptr, vptr []byte // ziplist fptr: key, vptr: value

	de *dict.Entry
	di *dict.Iterator
}

const (
	objHashKey   = 1
	objHashValue = 2
)

func hashTypeConvertZiplist(o *robj, enc uint32) {
	if enc == ObjEncodingZipList {
		/* Nothing to do */
	} else if enc == ObjEncodingHt {
		hi := hashTypeInitIterator(o)
		dt := dict.Create(hashDictType, nil)

		for hashTypeNext(hi) != C_ERR {
			key := hashTypeCurrentObjectNewSds(hi, objHashKey)
			value := hashTypeCurrentObjectNewSds(hi, objHashValue)
			if !dt.Add(unsafe.Pointer(&key), unsafe.Pointer(&value)) {
				panic("Ziplist corruption detected")
			}
		}
		hashTypeReleaseIterator(hi)
		o.setEncoding(ObjEncodingHt)
		o.ptr = unsafe.Pointer(dt)
	} else {
		panic("Unknown hash encoding")
	}
}

func hashTypeLength(o *robj) (l int) {
	if o.getEncoding() == ObjEncodingZipList {
		l = ziplist.Len(*(*[]byte)(o.ptr)) / 2 // kv
	} else if o.getEncoding() == ObjEncodingHt {
		l = int((*dict.Dict)(o.ptr).Size())
	} else {
		panic("Unknown hash encoding")
	}
	return l
}

func hashTypeCurrentObjectNewSds(hi *hashTypeIterator, what int) sds.SDS {
	var vstr []byte
	var vlen int
	var vll int64

	hashTypeCurrentObject(hi, what, &vstr, &vlen, &vll)

	if vstr != nil {
		return sds.NewLen(vstr)
	}
	return sds.NewLen(fmt.Sprintf("%d", vll))
}

func hashTypeCurrentObject(hi *hashTypeIterator, what int, vstr *[]byte, vlen *int, vll *int64) {
	if hi.encoding == ObjEncodingZipList {
		*vstr = nil
		hashTypeCurrentFromZiplist(hi, what, vstr, vlen, vll)
	} else if hi.encoding == ObjEncodingHt {
		ele := hashTypeCurrentFromHashTable(hi, what)
		*vstr = ele.BufData(0)
		*vlen = sds.Len(ele)
	} else {
		panic("Unknown hash encoding")
	}
}

func hashTypeCurrentFromHashTable(hi *hashTypeIterator, what int) sds.SDS {
	var p unsafe.Pointer
	if what&objHashKey > 0 {
		p = dict.GetKey(hi.de)
	} else {
		p = dict.GetVal(hi.de)
	}

	return *(*sds.SDS)(p)
}

func hashTypeCurrentFromZiplist(hi *hashTypeIterator, what int, vstr *[]byte, vlen *int, vll *int64) {
	if what&objHashKey > 0 {
		ziplist.Get(hi.fptr, vstr, vlen, vll)
	} else {
		ziplist.Get(hi.vptr, vstr, vlen, vll)
	}
}

func hashTypeReleaseIterator(hi *hashTypeIterator) {
	if hi.encoding == ObjEncodingHt {
		hi.di.Release()
	}
}

func hashTypeNext(hi *hashTypeIterator) error {
	if hi.encoding == ObjEncodingZipList {
		zl := *(*[]byte)(hi.subject.ptr)
		fptr := hi.fptr
		vptr := hi.vptr
		if fptr == nil {
			fptr = ziplist.Index(zl, 0)
		} else {
			fptr = ziplist.Next(zl, vptr)
		}
		if fptr == nil {
			return C_ERR
		}
		vptr = ziplist.Next(zl, fptr)
		hi.fptr = fptr
		hi.vptr = vptr
	} else if hi.encoding == ObjEncodingHt {
		if hi.de = hi.di.Next(); hi.de == nil {
			return C_ERR
		}
	} else {
		panic("Unknown hash encoding")
	}
	return C_OK
}

func hashTypeInitIterator(subject *robj) *hashTypeIterator {
	hi := new(hashTypeIterator)
	hi.subject = subject
	hi.encoding = int(subject.getEncoding())
	if hi.encoding == ObjEncodingZipList {
		hi.fptr = nil
		hi.vptr = nil
	} else if hi.encoding == ObjEncodingHt {
		hi.di = (*dict.Dict)(subject.ptr).GetIterator()
	} else {
		panic("Unknown hash encoding")
	}
	return hi
}

func createHashObject() *robj {
	zl := ziplist.New()
	o := createObject(ObjHash, zl)
	o.setEncoding(ObjEncodingZipList)
	return o
}
