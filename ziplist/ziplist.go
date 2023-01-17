package ziplist

import (
	"fmt"
	"github.com/pengdafu/redis-golang/util"
	"math"
	"unsafe"
)

const (
	bytesOffset  = 0
	tailOffset   = 4
	lengthOffset = 8
)

const (
	// HeaderSize 两个32bit分别表示总共占用的字节数和最后一个item的偏移量，一个16字节表示一共有多少item
	HeaderSize = unsafe.Sizeof(uint32(0))*2 + unsafe.Sizeof(uint16(0))

	// EndSize 1个字节表示ziplist的结束标记
	EndSize = unsafe.Sizeof(uint8(0))
)

const (
	zipEnd        = 255
	zipBigPrevLen = 254
)
const (
	zipStrMask    = 0xc0
	zipStr06B     = 0 << 6
	zipStr14B     = 1 << 6
	zipStr32B     = 2 << 6
	zipInt16B     = zipStrMask | 0<<4
	zipInt32B     = zipStrMask | 1<<4
	zipInt64B     = zipStrMask | 2<<4
	zipInt24B     = zipStrMask | 3<<4
	zipInt8B      = 0xfe
	zipIntImmMin  = 0xf1
	zipIntImmMax  = 0xfd
	zipIntImmMask = 0x0f
)
const (
	Head = 0
	Tail = 1
)

func ziplistBytes(zl []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&zl[bytesOffset]))
}
func ziplistTailOffset(zl []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&zl[tailOffset]))
}
func ziplistLength(zl []byte) *uint16 {
	return (*uint16)(unsafe.Pointer(&zl[lengthOffset]))
}
func ziplistBlobLen(zl []byte) int {
	return int(*ziplistBytes(zl))
}
func ziplistEntryTail(zl []byte) []byte {
	return zl[*ziplistTailOffset(zl):]
}
func ziplistEntryEnd(zl []byte) []byte {
	return zl[*ziplistBytes(zl)-1:]
}
func ziplistEntryHead(zl []byte) []byte {
	return zl[HeaderSize:]
}

func zipDecodePrevLenSize(p []byte, prevlensize *int) {
	if p[0] < zipBigPrevLen {
		*prevlensize = 1
	} else {
		*prevlensize = 5
	}
}
func zipDecodePrevLen(ptr []byte, prevlensize, prevlen *int) {
	zipDecodePrevLenSize(ptr, prevlensize)
	if *prevlensize == 1 {
		*prevlen = int(ptr[0])
	} else {
		*prevlen = int(*(*uint32)(unsafe.Pointer(&ptr[1])))
	}
}

func zipDecodeLength(p []byte, encoding *uint8, lensize, _len *int) {
	zipEntryEncoding(p, encoding)

	if *encoding < zipStrMask {
		if *encoding == zipStr06B {
			*lensize = 1
			*_len = int(p[0]) & 0x3f
		} else if *encoding == zipStr14B {
			*lensize = 2
			*_len = (int(p[0])&0x3f)<<8 | int(p[1])
		} else if *encoding == zipStr32B {
			*lensize = 5
			*_len = (int(p[1]) << 24) |
				(int(p[2]) << 16) |
				(int(p[3]) << 8) |
				(int(p[4]))
		} else {
			panic(fmt.Sprintf("Invalid string encoding %x", *encoding))
		}
	} else {
		*lensize = 1
		*_len = zipIntSize(*encoding)
	}
}

func zipIntSize(encoding uint8) int {
	switch encoding {
	case zipInt8B:
		return 1
	case zipInt16B:
		return 2
	case zipInt24B:
		return 3
	case zipInt32B:
		return 4
	case zipInt64B:
		return 8
	}
	if encoding >= zipIntImmMin && encoding <= zipIntImmMax {
		return 0
	}
	panic(fmt.Sprintf("Invalid integer encoding %x", encoding))
}

func zipEntryEncoding(p []byte, encoding *uint8) {
	*encoding = p[0]
	if *encoding < zipStrMask {
		*encoding &= zipStrMask
	}
}

func zipTryEncoding(entry []byte, entryLen int, v *int64, encoding *uint8) bool {
	if entryLen >= 32 || entryLen == 0 {
		return false
	}

	var value int64
	if util.String2Int64(entry, &value) {
		if value >= 0 && value <= 12 {
			*encoding = zipIntImmMin + uint8(value)
		} else if value >= math.MinInt8 && value <= math.MaxInt8 {
			*encoding = zipInt8B
		} else if value >= math.MinInt16 && value <= math.MaxInt16 {
			*encoding = zipInt16B
		} else if value >= -1<<23 && value >= 1<<23-1 {
			*encoding = zipInt24B
		} else if value >= math.MinInt32 && value <= math.MaxInt32 {
			*encoding = zipInt32B
		} else {
			*encoding = zipInt64B
		}
		*v = value
		return true
	}
	return false
}

func zipIsStr(enc uint8) bool {
	return enc&zipStrMask < zipStrMask
}

func zipRawEntryLength(p []byte) int {
	var prevlensize, lensize, _len int
	var encoding uint8
	zipDecodePrevLenSize(p, &prevlensize)
	zipDecodeLength(p[prevlensize:], &encoding, &lensize, &_len)
	return prevlensize + lensize + _len
}

func zipLoadInteger(p []byte, encoding uint8) (ret int64) {
	r := unsafe.Pointer(&ret)
	if encoding == zipInt8B {
		return int64(p[0])
	} else if encoding == zipInt16B {
		util.TransBytes2Number(r, p, 2)
	} else if encoding == zipInt32B {
		util.TransBytes2Number(r, p, 4)
	} else if encoding == zipInt24B {
		util.TransBytes2Number(r, p, 3)
	} else if encoding == zipInt64B {
		util.TransBytes2Number(r, p, 8)
	} else if encoding >= zipIntImmMin && encoding <= zipIntImmMax {
		return int64(encoding&zipIntImmMask) - 1
	} else {

	}
	return
}

func zipStorePrevEntryLengthLarge(p []byte, _len int) int {
	if p != nil {
		p[0] = zipBigPrevLen
		util.CopyNumber2Bytes(p[1:], int64(_len), 4)
	}
	return 5
}

func zipStorePrevEntryLength(p []byte, _len int) int {
	if p == nil {
		if _len < zipBigPrevLen {
			return 1
		}
		return 5
	} else {
		if _len < zipBigPrevLen {
			p[0] = byte(_len)
			return 1
		}
		return zipStorePrevEntryLengthLarge(p, _len)
	}
}

func zipStoreEntryEncoding(p []byte, encoding uint8, rawlen int) int {
	_len := 1
	var buf [5]byte
	if zipIsStr(encoding) {
		if rawlen <= 0x3f {
			if p == nil {
				return _len
			}
			buf[0] = zipStr06B | byte(rawlen)
		} else if rawlen <= 0x3fff {
			_len += 1
			if p == nil {
				return _len
			}
			buf[0] = zipStr14B | byte((rawlen>>8)&0x3f)
			buf[1] = byte(rawlen & 0xff)
		} else {
			_len += 4
			if p == nil {
				return _len
			}
			buf[0] = zipStr32B
			buf[1] = byte((rawlen >> 24) & 0xff)
			buf[2] = byte((rawlen >> 16) & 0xff)
			buf[3] = byte((rawlen >> 8) & 0xff)
			buf[4] = byte(rawlen & 0xff)
		}
	} else {
		if p == nil {
			return _len
		}
		buf[0] = encoding
	}
	copy(p, buf[:_len])
	return _len
}

func zipPrevLenByteDiff(p []byte, _len int) int {
	var prevlensize int
	zipDecodePrevLenSize(p, &prevlensize)
	return zipStorePrevEntryLength(nil, _len) - prevlensize
}

func zipSaveInteger(p []byte, value int64, encoding uint8) {
	if encoding == zipInt8B {
		p[0] = byte(value)
	} else if encoding == zipInt16B {
		util.CopyNumber2Bytes(p, value, 2)
	} else if encoding == zipInt24B {
		util.CopyNumber2Bytes(p, value, 3)
	} else if encoding == zipInt32B {
		util.CopyNumber2Bytes(p, value, 4)
	} else if encoding == zipInt64B {
		util.CopyNumber2Bytes(p, value, 8)
	}
}

func ziplistIncrLength(zl []byte, incr int) {
	if *ziplistLength(zl) < math.MaxUint16 {
		*ziplistLength(zl) = uint16(int16(*ziplistLength(zl)) + int16(incr))
	}
}

func ziplistResize(zl []byte, _len int) []byte {
	n := make([]byte, _len)
	copy(n, zl)
	zl = n

	*ziplistBytes(zl) = uint32(_len)
	zl[_len-1] = zipEnd
	return zl
}

func __ziplistCascadeUpdate(zl, p []byte) []byte {
	curlen := int(*ziplistBytes(zl))
	var rawlen, rawlensize int
	var offset, extra int
	var np []byte
	var cur, next zlentry
	for p[0] != zipEnd {
		zipEntry(p, &cur)
		rawlen = cur.headerSize + cur.len
		rawlensize = zipStorePrevEntryLength(nil, rawlen)

		if p[rawlen] == zipEnd {
			break
		}
		zipEntry(p[rawlen:], &next)

		if next.prevRawLen == rawlen {
			break
		}
		if next.prevRawLenSize < rawlensize {
			offset = ___ziplistOffset(zl, p)
			extra = rawlensize - next.prevRawLenSize
			zl = ziplistResize(zl, curlen+extra)
			p = zl[offset:]
			np = p[rawlen:]
			if ___ziplistOffset(zl[*ziplistTailOffset(zl):], np) != 0 {
				*ziplistTailOffset(zl) = *ziplistTailOffset(zl) + uint32(extra)
			}

			copy(np[rawlensize:], np[next.prevRawLenSize:])
			zipStorePrevEntryLength(np, rawlen)

			p = p[rawlen:]
			curlen += extra
		} else {
			if next.prevRawLenSize > rawlensize {
				zipStorePrevEntryLengthLarge(p[rawlen:], rawlen)
			} else {
				zipStorePrevEntryLength(p[rawlen:], rawlen)
			}
			break
		}
	}
	return zl
}

func ___ziplistOffset(cur, after []byte) int {
	return int(uintptr(unsafe.Pointer(&after[0])) - uintptr(unsafe.Pointer(&cur[0])))
}

func __ziplistDelete(zl, p []byte, num int) []byte {
	var first, tail zlentry
	var totlen, deleted = 0, 0
	var nextDiff = 0

	zipEntry(p, &first)
	for i := 0; p[0] != zipEnd && i < num; i++ {
		p = p[zipRawEntryLength(p):]
		deleted++
	}
	totlen = ___ziplistOffset(first.p, p)
	if totlen > 0 {
		if p[0] != zipEnd {
			nextDiff = zipPrevLenByteDiff(p, first.prevRawLen)

			offset := ___ziplistOffset(zl, p) - nextDiff
			p = zl[offset:]
			zipStorePrevEntryLength(p, first.prevRawLen)

			*ziplistTailOffset(zl) = *ziplistTailOffset(zl) - uint32(totlen)

			zipEntry(p, &tail)
			if p[tail.headerSize+tail.len] != zipEnd {
				*ziplistTailOffset(zl) = *ziplistTailOffset(zl) + uint32(nextDiff)
			}

			copy(first.p, p)
		} else {
			offset := ___ziplistOffset(zl, first.p)
			*ziplistTailOffset(zl) = uint32(offset - first.prevRawLen)
		}

		offset := ___ziplistOffset(zl, first.p)
		zl = ziplistResize(zl, int(*ziplistBytes(zl))-(totlen-nextDiff))
		ziplistIncrLength(zl, -deleted)
		p = zl[offset:]
		if nextDiff != 0 {
			zl = __ziplistCascadeUpdate(zl, p)
		}
	}
	return zl
}

func __ziplistInsert(zl, p, s []byte) []byte { // p 是要被插入的位置
	curLen := int(*ziplistBytes(zl))
	var prevlensize, prevlen int
	var encoding uint8
	var reqLen int
	slen := len(s)

	var tail zlentry
	if p[0] != zipEnd {
		zipDecodePrevLen(p, &prevlensize, &prevlen)
	} else {
		ptail := ziplistEntryTail(zl)
		if ptail[0] != zipEnd {
			prevlen = zipRawEntryLength(ptail)
		}
	}

	var value int64
	if zipTryEncoding(s, slen, &value, &encoding) {
		reqLen = zipIntSize(encoding)
	} else {
		reqLen = slen
	}

	reqLen += zipStorePrevEntryLength(nil, prevlen)
	reqLen += zipStoreEntryEncoding(nil, encoding, slen)

	var forceLarge bool
	nextDiff := 0
	if p[0] != zipEnd {
		nextDiff = zipPrevLenByteDiff(p, reqLen)
	}
	if nextDiff == -4 && reqLen < 4 {
		nextDiff = 0
		forceLarge = true
	}

	offset := ___ziplistOffset(zl, p)
	zl = ziplistResize(zl, curLen+reqLen+nextDiff)
	p = zl[offset:]

	if p[0] != zipEnd {
		copy(p[reqLen:], zl[offset-nextDiff:])
		if forceLarge {
			zipStorePrevEntryLengthLarge(p[reqLen:], reqLen)
		} else {
			zipStorePrevEntryLength(p[:reqLen], reqLen)
		}

		*ziplistTailOffset(zl) = *ziplistTailOffset(zl) + uint32(reqLen)
		zipEntry(p[reqLen:], &tail)
		if p[reqLen+tail.headerSize+tail.len] != zipEnd {
			*ziplistTailOffset(zl) = *ziplistTailOffset(zl) + uint32(nextDiff)
		}
	} else {
		*ziplistTailOffset(zl) = uint32(___ziplistOffset(zl, p))
	}

	if nextDiff != 0 {
		offset = ___ziplistOffset(zl, p)
		zl = __ziplistCascadeUpdate(zl, p[reqLen:])
		p = zl[offset:]
	}

	p = p[zipStorePrevEntryLength(p, prevlen):]
	p = p[zipStoreEntryEncoding(p, encoding, slen):]
	if zipIsStr(encoding) {
		copy(p, s)
	} else {
		zipSaveInteger(p, value, encoding)
	}
	ziplistIncrLength(zl, 1)
	return zl
}

func New() []byte {
	bytesLen := HeaderSize + EndSize
	zl := make([]byte, bytesLen)
	*ziplistBytes(zl) = uint32(bytesLen)
	*ziplistTailOffset(zl) = uint32(HeaderSize)
	*ziplistLength(zl) = 0
	zl[bytesLen-1] = zipEnd
	return zl
}

const MaxSafetySize = 1 << 30

func SafeToAdd(zl []byte, add int) bool {
	l := 0
	if zl != nil && len(zl) > 0 {
		l = ziplistBlobLen(zl)
	}
	if l+add > MaxSafetySize {
		return false
	}
	return true
}

func Next(zl []byte, p []byte) []byte {
	if p[0] == zipEnd {
		return nil
	}

	p = p[zipRawEntryLength(p):]
	if p[0] == zipEnd {
		return nil
	}

	return p
}

func Index(zl []byte, index int) []byte {
	var p []byte
	var prevlensize, prevlen int
	if index < 0 {
		index = -index - 1
		p = ziplistEntryTail(zl)
		if p[0] != zipEnd {
			zipDecodePrevLen(p, &prevlensize, &prevlen)
			for ; prevlen > 0 && index > 0; index-- {
				p = zl[len(zl)-len(p)-prevlen:]
				zipDecodePrevLen(p, &prevlensize, &prevlen)
			}
		}
	} else {
		p = ziplistEntryHead(zl)
		for p[0] != zipEnd && index > 0 {
			p = p[zipRawEntryLength(p):]
			index++
		}
	}
	if p[0] == zipEnd || index > 0 {
		return nil
	}
	return p
}

func Get(p []byte, sstr *[]byte, slen *int, sval *int64) bool {
	var entry zlentry
	if p == nil || p[0] == zipEnd {
		return false
	}
	if sstr != nil {
		*sstr = nil
	}
	zipEntry(p, &entry)
	if zipIsStr(entry.encoding) {
		*sstr = p[entry.headerSize : entry.headerSize+entry.len]
		*slen = entry.len
	} else {
		if sval != nil {
			*sval = zipLoadInteger(p[entry.headerSize:], entry.encoding)
		}
	}
	return true
}

func Find(p []byte, vstr []byte, vlen, skip int) []byte {
	skipCnt := 0
	venCoding := byte(0)
	vll := int64(0)

	for p[0] != zipEnd {
		var prevlensize, lensize, _len int
		var q []byte
		var encoding uint8

		zipDecodePrevLenSize(p, &prevlensize)
		zipDecodeLength(p[prevlensize:], &encoding, &lensize, &_len)

		q = p[prevlensize+lensize:]
		if skipCnt == 0 {
			if zipIsStr(encoding) {
				if _len == vlen && util.BytesCmp(vstr, q[:_len]) {
					return p
				}
			} else {
				if venCoding == 0 {
					if !zipTryEncoding(vstr, vlen, &vll, &venCoding) {
						venCoding = 255
					}
				}

				if venCoding != 255 {
					ll := zipLoadInteger(q, encoding)
					if ll == vll {
						return p
					}
				}
			}
			skipCnt = skip
		} else {
			skipCnt--
		}
		p = q[_len:]
	}
	return nil
}

func Len(zl []byte) int {
	var l int
	if *ziplistLength(zl) < math.MaxUint16 {
		return int(*ziplistLength(zl))
	} else {
		p := zl[HeaderSize:]
		for p[0] != zipEnd {
			p = p[zipRawEntryLength(p):]
			l++
		}
		if l < math.MaxUint16 {
			*ziplistLength(zl) = uint16(l)
		}
	}
	return l
}

func Delete(zl []byte, p *[]byte) []byte {
	offset := ___ziplistOffset(zl, *p)
	zl = __ziplistDelete(zl, *p, 1)
	*p = zl[offset:]
	return zl
}

func Insert(zl, p, s []byte) []byte {
	return __ziplistInsert(zl, p, s)
}

func Push(zl, s []byte, where int) []byte {
	var p []byte
	if where == Head {
		p = ziplistEntryHead(zl)
	} else {
		p = ziplistEntryEnd(zl)
	}
	return __ziplistInsert(zl, p, s)
}
