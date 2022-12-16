package dict

import "unsafe"

type dictEntry struct {
	key unsafe.Pointer
	v   struct {
		val unsafe.Pointer
		u64 uint64
		i64 int64
		d   float64
	}
	next *dictEntry
}
