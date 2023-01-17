package util

import "unsafe"

func CopyNumber2Bytes(dst []byte, src int64, size int) {
	p := unsafe.Pointer(&src)
	var pv []byte
	switch size {
	case 2:
		pv = (*[2]byte)(p)[:]
	case 3:
		pv = (*[3]byte)(p)[:]
	case 4:
		pv = (*[4]byte)(p)[:]
	case 8:
		pv = (*[8]byte)(p)[:]
	default:
		panic("Unknown size")
	}
	copy(dst, pv[:size])
}

func TransBytes2Number(dst unsafe.Pointer, src []byte, size int) {
	var pv []byte
	switch size {
	case 2:
		pv = (*[2]byte)(dst)[:]
	case 3:
		pv = (*[3]byte)(dst)[:]
	case 4:
		pv = (*[4]byte)(dst)[:]
	case 8:
		pv = (*[8]byte)(dst)[:]
	default:
		panic("Unknown size")
	}
	copy(pv, src)
}
