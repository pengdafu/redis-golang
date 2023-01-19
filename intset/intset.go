package intset

import (
	"math"
	"unsafe"
)

type IntSet struct {
	length   uint32
	contents []int8
	encoding uint8
}

const (
	EncInt16 = 2
	EncInt32 = 4
	EncInt64 = 8
)

func New() IntSet {
	is := IntSet{}
	is.encoding = EncInt16
	is.length = 0
	return is
}

func _intsetValueEncoding(v int64) uint8 {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return EncInt64
	} else if v < math.MinInt16 || v > math.MaxInt16 {
		return EncInt32
	} else {
		return EncInt16
	}
}

func (is *IntSet) set(pos int, value int64) {
	encoding := is.encoding
	if encoding == EncInt64 {
		*(*int64)(unsafe.Pointer(&is.contents[pos*EncInt64])) = value
	} else if encoding == EncInt32 {
		*(*int32)(unsafe.Pointer(&is.contents[pos*EncInt32])) = int32(value)
	} else {
		*(*int16)(unsafe.Pointer(&is.contents[pos*EncInt16])) = int16(value)
	}
}

func (is *IntSet) resize(_len uint32) {
	int8Arr := make([]int8, _len*uint32(is.encoding))
	copy(int8Arr, is.contents)
	is.contents = int8Arr
}

func (is *IntSet) getEncoded(pos int, enc uint8) int64 {
	if enc == EncInt64 {
		return *(*int64)(unsafe.Pointer(&is.contents[pos*EncInt64]))
	} else if enc == EncInt32 {
		return int64(*(*int32)(unsafe.Pointer(&is.contents[pos*EncInt32])))
	} else {
		return int64(*(*int16)(unsafe.Pointer(&is.contents[pos*EncInt16])))
	}
}

func (is *IntSet) get(pos int) int64 {
	return is.getEncoded(pos, is.encoding)
}

func (is *IntSet) upgradeAndAdd(value int64) *IntSet {
	curEnc := is.encoding
	newEnc := _intsetValueEncoding(value)

	length := int(is.length)
	prepend := 0
	if value < 0 {
		prepend = 1
	}

	is.encoding = newEnc
	is.resize(is.length + 1)

	for length > 0 {
		length--
		is.set(length+prepend, is.getEncoded(length, curEnc))
	}

	if prepend > 0 {
		is.set(0, value)
	} else {
		is.set(int(is.length), value)
	}
	is.length++
	return is
}

func (is *IntSet) search(value int64, pos *int) bool {
	min := 0
	max := int(is.length - 1)
	if is.length == 0 {
		if pos != nil {
			*pos = 0
		}
		return false
	}

	if value > is.get(max) {
		if pos != nil {
			*pos = int(is.length)
		}
		return false
	} else if value < is.get(0) {
		if pos != nil {
			*pos = 0
		}
		return false
	}

	var mid int
	var cur int64
	for max >= min {
		mid = (max + min) >> 1
		cur = is.get(mid)
		if value > cur {
			min = mid + 1
		} else if value < cur {
			max = mid - 1
		} else {
			break
		}
	}

	if value == cur {
		if pos != nil {
			*pos = mid
		}
		return true
	}
	if pos != nil {
		*pos = min
	}
	return false
}

func (is *IntSet) moveTail(from, to int) {
	encoding := is.encoding
	var src, dst int
	if encoding == EncInt64 {
		src = from * EncInt64
		dst = to * EncInt64
	} else if encoding == EncInt32 {
		src = from * EncInt32
		dst = to * EncInt32
	} else {
		src = from * EncInt16
		dst = to * EncInt16
	}
	copy(is.contents[dst:], is.contents[src:])
}

func (is *IntSet) Add(value int64, success *bool) *IntSet {
	valenc := _intsetValueEncoding(value)
	if success != nil {
		*success = true
	}

	var pos int
	if valenc > is.encoding {
		return is.upgradeAndAdd(value)
	} else {
		if is.search(value, &pos) {
			if success != nil {
				*success = false
			}
			return is
		}
		is.resize(is.length + 1)
		if pos < int(is.length) {
			is.moveTail(pos, pos+1)
		}
	}
	is.set(pos, value)
	is.length++
	return is
}

func (is *IntSet) Len() int {
	return int(is.length)
}

func (is *IntSet) Get(pos int, value *int64) bool {
	if pos < int(is.length) {
		*value = is.get(pos)
		return true
	}
	return false
}

func (is *IntSet) Remove(value int64, success *bool) *IntSet {
	valEnc := _intsetValueEncoding(value)
	if success != nil {
		*success = false
	}

	var pos int
	if valEnc <= is.encoding && is.search(value, &pos) {
		oldLen := is.length
		if success != nil {
			*success = true
		}
		if pos < int(oldLen)-1 {
			is.moveTail(pos+1, pos)
		}
		is.resize(oldLen - 1)
		is.length--
	}
	return is
}
