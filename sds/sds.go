package sds

import (
	"encoding/binary"
	"github.com/pengdafu/redis-golang/util"
	"strings"
	"unicode"
	"unsafe"
)

const (
	MAXPreAlloc = 1024 * 1024
)
const (
	Type5 = iota
	Type8
	Type16
	Type32
	Type64
	TypeMask = 7
	TypeBits = 3

	flagOffset = 0
	lenOffset  = 1
)

type sdshdr5 struct {
	buf   []byte
	flags uint8
}
type sdshdr8 struct {
	buf   []byte
	len   uint8 // alloc = len(buf) = cap(buf), len = used bytes
	flags uint8
}
type sdshdr16 struct {
	buf   []byte
	len   uint16
	flags uint8
}
type sdshdr32 struct {
	buf   []byte
	len   uint32
	flags uint8
}
type sdshdr64 struct {
	buf   []byte
	len   uint64
	flags uint8
}

type SDS struct {
	buf []byte
}

func sdsHdrSize(sdsType uint8) int {
	switch sdsType & TypeMask {
	case Type5:
		return int(unsafe.Sizeof(uint8(0))) // only flags uint8
	case Type8:
		return 2 * int(unsafe.Sizeof(uint8(0))) // flags uint8 + len uint8
	case Type16:
		return int(unsafe.Sizeof(uint8(0))) + int(unsafe.Sizeof(uint16(0))) // flags uint8 + len uint16
	case Type32:
		return int(unsafe.Sizeof(uint8(0))) + int(unsafe.Sizeof(uint32(0))) // flags uint8 + len uint32
	case Type64:
		return int(unsafe.Sizeof(uint8(0))) + int(unsafe.Sizeof(uint64(0))) // flags uint8 + len uint32
	}
	return 0
}

func sdsReqType(strSiz int) uint8 {
	if strSiz < 1<<5 {
		return Type5
	}
	if strSiz < 1<<8 {
		return Type8
	}
	if strSiz < 1<<16 {
		return Type16
	}
	if strSiz < 1<<32 {
		return Type32
	}
	return Type64
}

func sdsavail(s SDS) int {
	sdsType := s.buf[flagOffset] & TypeMask
	hdrSize := sdsHdrSize(sdsType)
	switch sdsType {
	case Type5:
		return 0
	case Type8:
		return cap(s.buf) - int(s.buf[lenOffset]) - hdrSize
	case Type16:
		return cap(s.buf) - int(binary.BigEndian.Uint16(s.buf[lenOffset:])) - hdrSize
	case Type32:
		return cap(s.buf) - int(binary.BigEndian.Uint32(s.buf[lenOffset:])) - hdrSize
	case Type64:
		return cap(s.buf) - int(binary.BigEndian.Uint64(s.buf[lenOffset:])) - hdrSize
	}
	return 0
}

func sdssetlen(s SDS, newlen int) {
	switch s.buf[flagOffset] & TypeMask {
	case Type5:
		s.buf[flagOffset] = Type5 | uint8(newlen)<<TypeBits
	case Type8:
		s.buf[lenOffset] = uint8(newlen)
	case Type16:
		binary.BigEndian.PutUint16(s.buf[lenOffset:], uint16(newlen))
	case Type32:
		binary.BigEndian.PutUint32(s.buf[lenOffset:], uint32(newlen))
	case Type64:
		binary.BigEndian.PutUint64(s.buf[lenOffset:], uint64(newlen))
	}
}

func isHexDigit(chars ...byte) bool {
	for _, c := range chars {
		if !(('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
			return false
		}
	}
	return true
}

func hexDigitToInt(b byte) byte {
	switch b {
	case '0':
		return 0
	case '1':
		return 1
	case '2':
		return 2
	case '3':
		return 3
	case '4':
		return 4
	case '5':
		return 5
	case '6':
		return 6
	case '7':
		return 7
	case '8':
		return 8
	case '9':
		return 9
	case 'a', 'A':
		return 10
	case 'b', 'B':
		return 11
	case 'c', 'C':
		return 12
	case 'd', 'D':
		return 13
	case 'e', 'E':
		return 14
	case 'f', 'F':
		return 15
	}
	return 0
}

// Empty 获取一个空的sds结构
func Empty() SDS {
	return NewLen("")
}

// sdsnewlen (init string, initlen int)
func NewLen(init string) SDS {
	initlen := len(init)
	sdsType := sdsReqType(initlen)
	if sdsType == Type5 && initlen == 0 {
		sdsType = Type8
	}

	hdrSize := sdsHdrSize(sdsType)

	if hdrSize+initlen < initlen {
		panic("overflow")
	}

	s := make([]byte, hdrSize+initlen) // s_malloc(hdrlen+initlen+1)
	s[flagOffset] = sdsType            // type5 will overwrite
	switch sdsType {
	case Type5:
		s[flagOffset] = sdsType | (byte(initlen) << TypeBits)
	case Type8:
		s[lenOffset] = byte(initlen)
	case Type16:
		binary.BigEndian.PutUint16(s[lenOffset:], uint16(initlen))
	case Type32:
		binary.BigEndian.PutUint32(s[lenOffset:], uint32(initlen))
	case Type64:
		binary.BigEndian.PutUint64(s[lenOffset:], uint64(initlen))
	}

	if initlen > 0 && init != "" {
		copy(s[hdrSize:], init)
	}

	// s[init_len]=`\0`

	return SDS{s}
}

// Len 返回sds长度，redis 判断sds类型获取不同的结构，从而获取长度
func Len(s SDS) int {
	sdsType := s.buf[flagOffset] & TypeMask
	switch sdsType {
	case Type5:
		return int(s.buf[flagOffset] >> TypeBits)
	case Type8:
		return int(s.buf[lenOffset])
	case Type16:
		return int(binary.BigEndian.Uint16(s.buf[lenOffset:]))
	case Type32:
		return int(binary.BigEndian.Uint32(s.buf[lenOffset:]))
	case Type64:
		return int(binary.BigEndian.Uint64(s.buf[lenOffset:]))
	}
	return 0
}

func MakeRoomFor(s SDS, addLen int) SDS {
	avail := sdsavail(s)
	if avail > addLen {
		return s
	}

	oldLen := Len(s)
	reqLen := oldLen + addLen
	newLen := reqLen

	if newLen < oldLen {
		panic("overflow")
	}

	if newLen < MAXPreAlloc {
		newLen *= 2
	} else {
		newLen += MAXPreAlloc
	}
	oldType := s.buf[flagOffset] & TypeMask
	newType := sdsReqType(newLen)
	if newType == Type5 {
		newType = Type8
	}

	hdrLen := sdsHdrSize(newType)
	if (hdrLen + newLen + 1) < reqLen {
		panic("overflow")
	}

	buf := make([]byte, newLen+hdrLen)
	if oldType == newType {
		copy(buf, s.buf)
	} else {
		copy(buf[hdrLen:], s.buf[sdsHdrSize(oldType):])
		buf[flagOffset] = newType
		sdssetlen(SDS{buf}, oldLen)
	}
	s.buf = buf
	return s
}

func IncrLen(s SDS, incr int) {
	sdsType := s.buf[flagOffset] & TypeMask
	switch sdsType {
	case Type5:
		oldLen := s.buf[flagOffset] >> TypeBits
		s.buf[flagOffset] = Type5 | (uint8(incr)+oldLen)<<TypeBits
	case Type8:
		s.buf[lenOffset] += uint8(incr)
	case Type16:
		binary.BigEndian.PutUint16(s.buf[lenOffset:], binary.BigEndian.Uint16(s.buf[lenOffset:])+uint16(incr))
	case Type32:
		binary.BigEndian.PutUint32(s.buf[lenOffset:], binary.BigEndian.Uint32(s.buf[lenOffset:])+uint32(incr))
	case Type64:
		binary.BigEndian.PutUint64(s.buf[lenOffset:], binary.BigEndian.Uint64(s.buf[lenOffset:])+uint64(incr))
	}
}

// BufData 返回实际数据
func (s SDS) BufData(offset int) []byte {
	end := Len(s)
	hdrSize := sdsHdrSize(s.buf[flagOffset])
	return s.buf[hdrSize+offset : hdrSize+offset+end]
}

// Buf 返回除hdr的buf
func (s SDS) Buf(offset int) []byte {
	return s.buf[sdsHdrSize(s.buf[flagOffset])+offset:]
}

func SplitArgs(s SDS) (argv []SDS, argc int) {
	p := s.BufData(0)

	var vector []SDS
	for { // while(1)
		for len(p) > 0 && unicode.IsSpace(rune(p[0])) {
			p = p[1:]
		}

		if len(p) > 0 {
			inq := false  // 如果处于双引号中，设置为1
			insq := false // 如果处于单引号中，设置为1
			done := false
			current := Empty()
			for !done {
				if inq {
					if len(p) >= 4 && p[0] == '\\' && p[1] == 'x' && isHexDigit(p[2], p[3]) {
						current = Catlen(current, []byte{hexDigitToInt(p[2])*16 + hexDigitToInt(p[3])}, 1)
						p = p[3:] // p+=3
					} else if len(p) >= 2 && p[0] == '\\' {
						var c byte
						p = p[1:]
						switch p[0] {
						case 'n':
							c = '\n'
						case 'r':
							c = '\r'
						case 't':
							c = '\t'
						case 'b':
							c = '\b'
						case 'a':
							c = '\a'
						default:
							c = p[0]
						}
						current = Catlen(current, []byte{c}, 1)
					} else if p[0] == '"' {
						if len(p) >= 2 && !unicode.IsSpace(rune(p[1])) {
							return nil, 0 // goto err
						}
						done = true
					} else if len(p) == 0 {
						return nil, 0
					} else {
						current = Catlen(current, p, 1)
					}
				} else if insq {
					if len(p) >= 2 && p[0] == '\\' && p[1] == '\'' {
						p = p[1:]
						current = Catlen(current, []byte{'\''}, 1)
					} else if p[0] == '\'' {
						if len(p) >= 2 && !unicode.IsSpace(rune(p[1])) {
							return nil, 0 // goto err
						}
						done = true
					} else if len(p) == 0 {
						return nil, 0
					} else {
						current = Catlen(current, p, 1)
					}
				} else {
					if len(p) == 0 { // case \0
						done = true
						continue
					}
					switch p[0] {
					case ' ', '\n', '\r', '\t' /*, '\0'*/ :
						done = true
					case '"':
						inq = true
					case '\'':
						insq = true
					default:
						current = Catlen(current, p, 1)
					}
				}
				if len(p) > 0 {
					p = p[1:]
				}
			}

			vector = append(vector, current)
			argc++
		} else {
			return vector, argc
		}
	}
}

func Catlen(s SDS, p []byte, l int) SDS {
	curLen := Len(s)

	s = MakeRoomFor(s, l)
	copy(s.Buf(curLen), p[:l])
	sdssetlen(s, curLen+l)

	return s
}

func Range(s SDS, start, end int) {
	oldLen := Len(s)
	if oldLen == 0 { // sds 还没使用
		return
	}

	if start < 0 {
		start += oldLen
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end += oldLen
		if end < 0 {
			end = 0
		}
	}

	newlen := 0
	if start <= end {
		newlen = end - start + 1
	}
	if newlen != 0 {
		if start > oldLen {
			newlen = 0
		} else if end >= oldLen {
			end = oldLen - 1
			newlen = 0
			if start <= end {
				newlen = end - start + 1
			}
		}
	} else {
		start = 0
	}

	if start > 0 && newlen > 0 {
		copy(s.Buf(0), s.Buf(start)[:newlen])
	}
	sdssetlen(s, newlen)
}

func Clear(s SDS) {
	sdssetlen(s, 0)
}

func CatPrintf(ss ...SDS) string {
	ret := make([]string, 0, len(ss))
	for _, s := range ss {
		ret = append(ret, util.Bytes2String(s.BufData(0)))
	}
	return strings.Join(ret, ",")
}
