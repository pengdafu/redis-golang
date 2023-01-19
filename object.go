package main

import (
	"fmt"
	"github.com/pengdafu/redis-golang/dict"
	"github.com/pengdafu/redis-golang/intset"
	"github.com/pengdafu/redis-golang/sds"
	"github.com/pengdafu/redis-golang/util"
	"math"
	"unsafe"
)

const (
	typeBitMask       = 0xF
	encodingMask      = 0xF0
	lruBitMask        = math.MaxUint32 >> 8 << 8
	lruBitOffset      = 8
	encodingBitOffset = 4
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
	ObjSharedRefCount       = math.MaxInt
	ObjStaticRefCount       = ObjSharedRefCount - 1
	ObjFirstSpecialRefCount = ObjStaticRefCount
)

type robj struct {
	refCount int
	ptr      unsafe.Pointer //*int|*sds.SDS

	// 4bit type, 4bit encoding,
	// 24bit lru(lru time or lfu data(8bit frq and 16bit time))
	__ uint32
}

func (robj *robj) getType() int {
	return int(robj.__ & typeBitMask)
}

func (robj *robj) getEncoding() uint32 {
	return robj.__ & encodingMask >> encodingBitOffset
}

func (robj *robj) getLru() uint32 {
	return robj.__ >> lruBitOffset
}
func (robj *robj) setType(typ int) {
	robj.__ |= uint32(typ & typeBitMask)
}
func (robj *robj) setEncoding(encoding uint32) {
	robj.__ |= encoding << encodingBitOffset & encodingMask
}
func (robj *robj) setLru(lru uint32) {
	robj.__ |= lru << lruBitOffset & lruBitMask
}

func (robj *robj) makeObjectShared() *robj {
	robj.refCount = ObjSharedRefCount
	return robj
}

func isSdsRepresentableAsLongLong(s sds.SDS, llval *int64) bool {
	return util.String2Int64(s.BufData(0), llval)
}

type objPtrType interface {
	sds.SDS | int | int64 | []byte | intset.IntSet | dict.Dict
}

func createObject[T objPtrType](typ int, ptr T) *robj {
	o := new(robj)
	o.setType(typ)
	o.setEncoding(ObjEncodingRaw)
	o.ptr = unsafe.Pointer(&ptr)
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

func createIntsetObject() *robj {
	is := intset.New()
	o := createObject(ObjSet, is)
	o.setEncoding(ObjEncodingIntSet)
	return o
}

func createSetObject() *robj {
	dt := dict.Create(setDictType, nil)
	o := createObject(ObjSet, *dt)
	o.setEncoding(ObjEncodingHt)
	return o
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
func createStringObjFromLongLongForValue(v int64) *robj {
	return createStringObjectFromLongLongWithOptions(v, 1)
}
func createStringObjectFromLongLongWithOptions(v int64, vobj int) *robj {
	if server.maxMemory == 0 || server.maxMemoryPolicy&MaxMemoryFlagNoSharedIntegers == 0 {
		vobj = 0
	}

	if v >= 0 && v < ObjSharedIntegers && vobj == 0 {
		shared.integers[v].incrRefCount()
		return shared.integers[v]
	} else {
		o := createObject(ObjString, v)
		o.setEncoding(ObjEncodingInt)
		o.ptr = unsafe.Pointer(&v)
		return o
	}
}

func (o *robj) sdsEncodedObject() bool {
	ed := o.getEncoding()
	return ed == ObjEncodingRaw || ed == ObjEncodingEmbStr
}

func (o *robj) decrRefCount() {
	if o.refCount == 1 {
		switch o.getType() {

		}
		//zfree(o)
	} else {
		if o.refCount <= 0 {
			panic("decrRefCount against refCount <= 0")
		}
		if o.refCount != ObjSharedRefCount {
			o.refCount--
		}
	}
}

func (o *robj) incrRefCount() {
	if o.refCount < ObjFirstSpecialRefCount {
		o.refCount++
	} else {
		if o.refCount == ObjSharedRefCount {
			// nothing to do
		} else if o.refCount == ObjStaticRefCount {
			panic("You tried to retain an object allocated in the stack")
		}
	}
}

func (o *robj) getDecodedObject() *robj {
	if o.sdsEncodedObject() {
		o.incrRefCount()
		return o
	}

	if o.getType() == ObjString && o.getEncoding() == ObjEncodingInt {
		llStr := fmt.Sprintf("%v", *(*int)(o.ptr))
		return createStringObject(llStr)
	}
	panic("Unknown encoding type")
}

func (o *robj) tryObjectEncoding() *robj {
	if !o.sdsEncodedObject() {
		return o
	}

	s := *(*sds.SDS)(o.ptr)
	if o.refCount > 1 {
		return o
	}

	slen := sds.Len(s)
	var value int64
	if slen <= 20 && util.String2Int64(s.BufData(0), &value) {
		if (server.maxMemory == 0 || server.maxMemoryPolicy&MaxMemoryFlagNoSharedIntegers == 0) &&
			value >= 0 && value < ObjSharedIntegers {
			o.decrRefCount()
			shared.integers[value].incrRefCount()
			return shared.integers[value]
		} else {
			if o.getEncoding() == ObjEncodingRaw {
				o.setEncoding(ObjEncodingInt)
				o.ptr = unsafe.Pointer(&value)
				return o
			} else if o.getEncoding() == ObjEncodingEmbStr {
				o.decrRefCount()
				return createStringObjFromLongLongForValue(value)
			}
		}
	}

	if slen <= ObjEncodingEmbStrLimitSize {
		if o.getEncoding() == ObjEncodingEmbStr {
			return o
		}
		o.decrRefCount()
		return createEmbeddedStringObject(s.BufData(0))
	}
	o.trimStringObjectIfNeeded()
	return o
}

func (o *robj) trimStringObjectIfNeeded() {
	s := *(*sds.SDS)(o.ptr)
	if o.getEncoding() == ObjEncodingRaw && sds.Avail(s) > sds.Len(s)/10 {
		n := sds.RemoveFreeSpace(s)
		o.ptr = unsafe.Pointer(&n)
	}
}

func (o *robj) getLongLongFromObjectOrReply(c *Client, target *int64, msg string) error {
	var value int64
	if o.getLongLongFromObject(&value) != C_OK {
		if msg != "" {
			addReplyError(c, msg)
		} else {
			addReplyError(c, "value is not an integer or out of range")
		}
		return C_ERR
	}
	*target = value
	return C_OK
}
func (o *robj) getLongLongFromObject(target *int64) error {
	var value int64
	if o == nil {
		value = 0
	} else {
		if o.sdsEncodedObject() {
			if !util.String2Int64((*sds.SDS)(o.ptr).BufData(0), &value) {
				return C_ERR
			}
		} else if o.getEncoding() == ObjEncodingInt {
			value = int64(*(*int)(o.ptr))
		} else {
			panic("Unknown string encoding")
		}
	}
	if target != nil {
		*target = value
	}
	return C_OK
}

func (o *robj) stringObjectLen() int {
	if o.getType() != ObjString {
		panic("not string obj")
	}

	if o.sdsEncodedObject() {
		return sds.Len(*(*sds.SDS)(o.ptr))
	}
	return len(fmt.Sprintf("%v", *(*int)(o.ptr)))
}

func (o *robj) checkType(c *Client, typ int) bool {
	if o.getType() != typ {
		addReply(c, shared.wrongTypeErr)
		return true
	}
	return false
}
