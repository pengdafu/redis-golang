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

}

func hdelCommand(c *Client) {

}

func hgetallCommand(c *Client) {

}

func hkeysCommand(c *Client) {

}

func hvalsCommand(c *Client) {

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
