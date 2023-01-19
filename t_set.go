package main

import (
	"github.com/pengdafu/redis-golang/dict"
	"github.com/pengdafu/redis-golang/intset"
	"github.com/pengdafu/redis-golang/sds"
	"unsafe"
)

func saddCommand(c *Client) {
	var set *robj
	set = c.db.lookupKeyWrite(c.argv[1])
	if set == nil {
		set = setTypeCreate(*(*sds.SDS)(c.argv[2].ptr))
		c.db.dbAdd(c.argv[1], set)
	} else if set.checkType(c, ObjSet) {
		return
	}

	var added int
	for j := 2; j < c.argc; j++ {
		if setTypeAdd(set, c.argv[j].ptr) {
			added++
		}
	}
	if added > 0 {
		signalModifiedKey(c, c.db, c.argv[1])
		notifyKeySpaceEvent(notifySet, "sadd", c.argv[1], c.db.id)
	}
	server.dirty++
	addReplyLongLong(c, added)
}

func setTypeAdd(subject *robj, value unsafe.Pointer) bool {
	if subject.getEncoding() == ObjEncodingHt {
		ht := (*dict.Dict)(subject.ptr)
		de := ht.AddRaw(value, nil)
		if de != nil {
			dict.SetKey(de, value)
			dict.SetVal(de, nil)
			return true
		}
	} else if subject.getEncoding() == ObjEncodingIntSet {
		sdsValue := *(*sds.SDS)(value)
		var llval int64
		if isSdsRepresentableAsLongLong(sdsValue, &llval) {
			var success bool
			is := (*intset.IntSet)(subject.ptr).Add(llval, &success)
			subject.ptr = unsafe.Pointer(is)
			if success {
				maxEntries := server.setMaxIntSetEntries
				if maxEntries >= 1<<30 {
					maxEntries = 1 << 30
				}
				if is.Len() > maxEntries {
					setTypeConvert(subject, ObjEncodingHt)
				}
				return true
			}
		} else {
			setTypeConvert(subject, ObjEncodingHt)
			(*dict.Dict)(subject.ptr).Add(value, nil)
			return true
		}
	} else {
		panic("Unknown set encoding")
	}
	return false
}

func setTypeCreate(value sds.SDS) *robj {
	if isSdsRepresentableAsLongLong(value, nil) {
		return createIntsetObject()
	}
	return createSetObject()
}

type setTypeIterator struct {
	subject  *robj
	encoding int
	ii       int
	di       *dict.Iterator
}

func setTypeConvert(setobj *robj, enc uint32) {
	if enc != ObjEncodingHt {
		panic("Unsupported set conversion")
	}

	d := dict.Create(setDictType, nil)
	is := (*intset.IntSet)(setobj.ptr)
	d.Expand(int64(is.Len()))

	si := setTypeInitIterator(setobj)
	var element sds.SDS
	var intele int64
	for setTypeNext(si, &element, &intele) != -1 {
		element := sds.FromLongLong(intele)
		d.Add(unsafe.Pointer(&element), nil)
	}

	setTypeReleaseIterator(si)

	setobj.setEncoding(ObjEncodingHt)
	setobj.ptr = unsafe.Pointer(d)
}

func setTypeReleaseIterator(si *setTypeIterator) {
	if si.encoding == ObjEncodingHt {
		si.di.Release()
	}
}

func setTypeNext(si *setTypeIterator, sdsele *sds.SDS, llele *int64) int {
	if si.encoding == ObjEncodingHt {
		de := si.di.Next()
		if de == nil {
			return -1
		}
		*sdsele = *(*sds.SDS)(dict.GetKey(de))
		*llele = -123456789
	} else if si.encoding == ObjEncodingIntSet {
		is := (*intset.IntSet)(si.subject.ptr)
		ii := si.ii
		si.ii++
		if !is.Get(ii, llele) {
			return -1
		}
	} else {
		panic("Wrong set encoding in setTypeNext")
	}
	return si.encoding
}

func setTypeInitIterator(subject *robj) *setTypeIterator {
	si := new(setTypeIterator)
	si.subject = subject
	si.encoding = int(subject.getEncoding())
	if si.encoding == ObjEncodingHt {
		si.di = (*dict.Dict)(subject.ptr).GetIterator()
	} else if si.encoding == ObjEncodingIntSet {
		si.ii = 0
	} else {
		panic("Unknown set encoding")
	}
	return si
}

func sremCommand(c *Client) {
	var set *robj
	if set = lookupKeyWriteOrReply(c, c.argv[1], shared.czero); set == nil || set.checkType(c, ObjSet) {
		return
	}

	var deleted int
	var keyRemoved bool
	for j := 2; j < c.argc; j++ {
		if setTypeRemove(set, c.argv[j].ptr) {
			deleted++
			if setTypeSize(set) == 0 {
				dbDelete(c.db, c.argv[1])
				keyRemoved = true
				break
			}
		}
	}

	if deleted > 0 {
		signalModifiedKey(c, c.db, c.argv[1])
		notifyKeySpaceEvent(notifySet, "srem", c.argv[1], c.db.id)
		if keyRemoved {
			notifyKeySpaceEvent(notifyGeneric, "del", c.argv[1], c.db.id)
		}
		server.dirty++
	}
	addReplyLongLong(c, deleted)
}

func setTypeRemove(setobj *robj, value unsafe.Pointer) bool {
	var llval int64
	if setobj.getEncoding() == ObjEncodingHt {
		d := (*dict.Dict)(setobj.ptr)
		if d.Delete(value) {
			if htNeedResize(d) {
				d.Resize()
			}
			return true
		}
	} else if setobj.getEncoding() == ObjEncodingIntSet {
		if isSdsRepresentableAsLongLong(*(*sds.SDS)(value), &llval) {
			var success bool
			is := (*intset.IntSet)(setobj.ptr)
			setobj.ptr = unsafe.Pointer(is.Remove(llval, &success))
			if success {
				return true
			}
		}
	} else {
		panic("Unknown set encoding")
	}
	return false
}

func setTypeSize(subject *robj) int {
	if subject.getEncoding() == ObjEncodingHt {
		return int((*dict.Dict)(subject.ptr).Size())
	} else if subject.getEncoding() == ObjEncodingIntSet {
		return (*intset.IntSet)(subject.ptr).Len()
	} else {
		panic("Unknown set encoding")
	}
}

func scardCommand(c *Client) {
	var set *robj
	if set = lookupKeyReadOrReply(c, c.argv[1], shared.czero); set == nil || set.checkType(c, ObjSet) {
		return
	}

	addReplyLongLong(c, setTypeSize(set))
}

func sinterCommand(c *Client) {

}

func sinterGenericCommand(c *Client, setkeys []*robj, setnum int, dstkey *robj) {

}