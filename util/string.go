package util

import (
	"bytes"
	"math/rand"
	"strconv"
	"strings"
	"unsafe"
)

func String2Bytes(str string) []byte {
	x := *(*[2]uintptr)(unsafe.Pointer(&str))
	b := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&b))
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func StrCmp[T []byte | string](s T, d string) bool {
	if string(s) == d {
		return true
	}
	return false
}

func StrCaseCmp[T []byte | string](s T, d string) bool {
	t := string(s)
	return strings.ToLower(t) == strings.ToLower(d)
}

func BytesCmp(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}

	for i := 0; i < len(key2); i++ {
		if key1[i] != key2[i] {
			return false
		}
	}
	return true
}

func BytesCaseCmp(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}

	key1, key2 = bytes.ToLower(key1), bytes.ToLower(key2)

	for i := 0; i < len(key2); i++ {
		if key1[i] != key2[i] {
			return false
		}
	}
	return true
}

func GetRandomBytes(needLen int) []byte {
	ret := make([]byte, needLen)
	for i := 0; i < needLen; i++ {
		ret[i] = byte(rand.Intn(255))
	}
	return ret
}

func String2Int64[T []byte | string](str T, v *int64) bool {
	i, err := strconv.ParseInt(string(str), 10, 64)
	if err != nil {
		return false
	}

	if v == nil {
		return true
	}

	*v = i
	return true
}
