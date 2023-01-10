package dict

import "unsafe"

type Entry struct {
	key unsafe.Pointer
	v   struct {
		val unsafe.Pointer
		u64 uint64
		s64 int64
		d   float64
	}
	next *Entry
}

func (e *Entry) Next() *Entry {
	return e.next
}
