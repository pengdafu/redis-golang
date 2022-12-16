package main

import (
	"github.com/pengdafu/redis-golang/sds"
	"github.com/pengdafu/redis-golang/util"
	"math"
)

const (
	typeBitMask  = 0xF
	encodingMask = 0xF0
	lruBitMask   = math.MaxUint32 >> 8 << 8
	lruBitOffset = 8
)

const (
	ObjString = iota
	ObjList
	ObjSet
	ObjZSet
	ObjHash
	ObjModule
	ObjStream
)
const (
	ObjEncodingRaw = iota
	ObjEncodingInt
	ObjEncodingHt
	ObjEncodingZipMap
	ObjEncodingLinkedList
	ObjEncodingZipList
	ObjEncodingIntSet
	ObjEncodingSkipList
	ObjEncodingEmbStr
	ObjEncodingQuickList
	ObjEncodingStream // listpack
)

const (
	ObjSharedRefCount = math.MaxInt
)

type robj struct {
	refCount int
	ptr      interface{}

	// 4bit type, 4bit encoding,
	// 24bit lru(lru time or lfu data(8bit frq and 16bit time))
	__ uint32
}

func (robj *robj) getType() int {
	return int(robj.__ & typeBitMask)
}

func (robj *robj) getEncoding() int {
	return int(robj.__ & encodingMask)
}

func (robj *robj) getLru() int {
	return int(robj.__ >> lruBitOffset)
}
func (robj *robj) setType(typ int) {
	robj.__ |= uint32(typ & typeBitMask)
}
func (robj *robj) setEncoding(encoding uint32) {
	robj.__ |= encoding << typeBitMask & encodingMask
}
func (robj *robj) setLru(lru uint32) {
	robj.__ |= lru << lruBitOffset & lruBitMask
}

func (robj *robj) makeObjectShared() *robj {
	robj.refCount = ObjSharedRefCount
	return robj
}

func createObject(typ int, ptr interface{}) *robj {
	o := new(robj)
	o.setType(typ)
	o.setEncoding(ObjEncodingRaw)
	o.ptr = ptr
	o.refCount = 1

	if server.maxMemoryPolicy&MaxMemoryFlagLfu > 0 {
		o.setLru(uint32(LFUGetTimeInMinutes()<<lruBitOffset | LfuInitVal))
	} else {
		o.setLru(LRU_CLOCK())
	}
	return o
}

const (
	ObjEncodingEmbStrLimitSize = 44
)

func createStringObject(ptr string) *robj {
	if len(ptr) <= ObjEncodingEmbStrLimitSize {
		return createEmbeddedStringObject(util.String2Bytes(ptr))
	} else {
		return createRawStringObject(util.String2Bytes(ptr))
	}
}

// 说明长度肯定小于等于44
func createEmbeddedStringObject(ptr []byte) *robj {
	/**
	embStr 是指，robj和ptr对应的sds一起申请一个连续的内存。
	robj *o = zmalloc(sizeof(robj)+sizeof(struct sdshdr8)+len+1);
	struct sdshdr8 *sh = (void*)(o+1);
	*/
	o := createObject(ObjString, sds.NewLen(util.Bytes2String(ptr)))
	o.setEncoding(ObjEncodingEmbStr)
	return o
}
func createRawStringObject(ptr []byte) *robj {
	return createObject(ObjString, sds.NewLen(util.Bytes2String(ptr)))
}

func (o *robj) sdsEncodedObject() bool {
	ed := o.getEncoding()
	return ed == ObjEncodingRaw || ed == ObjEncodingEmbStr
}
