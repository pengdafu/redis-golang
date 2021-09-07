package pkg

import "unsafe"

func String2Bytes(str string) []byte {
	x := *(*[2]uintptr)(unsafe.Pointer(&str))
	b := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&b))
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
