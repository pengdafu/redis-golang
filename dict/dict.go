package dict

import (
	"github.com/dchest/siphash"
	"math"
	"unsafe"
)

var dictSeedKey []byte

var (
	dictCanResize        = false
	dictForceResizeRatio = int64(5)
	dictInitSize         = int64(4)
)

type Dict struct {
	typ       *Type
	privData  interface{}
	ht        [2]dictHt
	rehashIdx int64 // -1 表示没有进行rehash
	iterators uint64
}

type Type struct {
	HashFunction  func(key unsafe.Pointer) uint64
	KeyDup        func(privData interface{}, key unsafe.Pointer) unsafe.Pointer
	ValDup        func(privData interface{}, obj unsafe.Pointer) unsafe.Pointer
	KeyCompare    func(privData interface{}, key1, key2 unsafe.Pointer) bool
	KeyDestructor func(privData interface{}, key unsafe.Pointer)
	ValDestructor func(privData interface{}, obj unsafe.Pointer)
}

func Create(tp *Type, privData interface{}) *Dict {
	d := new(Dict)
	d.init(tp, privData)
	return d
}

func (dict *Dict) init(typ *Type, privData interface{}) {
	dict.ht[0].reset()
	dict.ht[1].reset()

	dict.typ = typ
	dict.privData = privData
	dict.rehashIdx = -1
	dict.iterators = 0
}
func (dict *Dict) addRaw(key unsafe.Pointer, existing **dictEntry) *dictEntry {
	if dict.isRehashing() {
		dict.rehashStep()
	}

	var index int
	if index = dict.keyIndex(key, dict.hashKey(key), existing); index == -1 {
		return nil
	}

	ht := &dict.ht[0]
	if dict.isRehashing() {
		ht = &dict.ht[1]
	}
	entry := new(dictEntry)
	entry.next = ht.table[index]
	ht.table[index] = entry
	ht.used++

	dict.setKey(entry, key)
	return entry
}

func (dict *Dict) setKey(entry *dictEntry, key unsafe.Pointer) {
	if dict.typ.KeyDup != nil {
		entry.key = dict.typ.KeyDup(dict.privData, key)
	} else {
		entry.key = key
	}
}
func (dict *Dict) setVal(entry *dictEntry, obj unsafe.Pointer) {
	if dict.typ.ValDup != nil {
		entry.v.val = dict.typ.ValDup(dict.privData, obj)
	} else {
		entry.v.val = obj
	}
}
func (dict *Dict) getVal(entry *dictEntry) unsafe.Pointer {
	return entry.v.val
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashIdx != -1
}

func (dict *Dict) expand(size int64) bool {
	if dict.isRehashing() || dict.ht[0].used > size {
		return false
	}
	realSize := dict.nextPower(size)
	if realSize == dict.ht[0].size {
		return false
	}

	n := dictHt{}
	n.size = realSize
	n.sizeMask = uint64(realSize - 1)
	n.used = 0
	n.table = make([]*dictEntry, realSize)
	if dict.ht[0].table == nil {
		dict.ht[0] = n
		return true
	}
	dict.ht[1] = n
	dict.rehashIdx = 0
	return true
}

func (dict *Dict) nextPower(size int64) int64 {
	if size >= math.MaxInt64 {
		return math.MaxInt64
	}
	i := dictInitSize
	for {
		if i >= size {
			return i
		}
		i *= 2
	}
}

func (dict *Dict) expandIfNeeded() bool {
	if dict.isRehashing() {
		return true
	}

	if dict.ht[0].size == 0 {
		return dict.expand(dictInitSize)
	}

	if dict.ht[0].used >= dict.ht[0].size && (dictCanResize || dict.ht[0].used/dict.ht[0].size >= dictForceResizeRatio) {
		return dict.expand(dict.ht[0].used * 2)
	}
	return true
}

func (dict *Dict) keyIndex(key unsafe.Pointer, hash uint64, existing **dictEntry) int {
	if existing != nil {
		*existing = nil
	}

	if !dict.expandIfNeeded() {
		return -1
	}
	var idx int
	for table := 0; table <= 1; table++ {
		idx = int(hash & dict.ht[table].sizeMask)
		he := dict.ht[table].table[idx]
		for he != nil {
			if key == he.key || dict.compareKey(key, he.key) {
				if existing != nil {
					*existing = he
					return -1
				}
			}
			he = he.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return idx
}

func (dict *Dict) compareKey(key1, key2 unsafe.Pointer) bool {
	return dict.typ.KeyCompare(dict.privData, key2, key1)
}
func (dict *Dict) hashKey(key unsafe.Pointer) uint64 {
	return dict.typ.HashFunction(key)
}

// todo rehash
func (dict *Dict) rehashStep() {
	if dict.iterators == 0 {
		dict.rehash(1)
	}
}

func (dict *Dict) rehash(n int) {
	emptyVisit := 10 * n
	if !dict.isRehashing() {
		return
	}

	for ; n > 0 && dict.ht[0].used != 0; n-- {
		for dict.ht[0].table[dict.rehashIdx] == nil {
			dict.rehashIdx++
			emptyVisit--
			if emptyVisit == 0 {
				return
			}
		}
		de := dict.ht[0].table[dict.rehashIdx]
		for de != nil {
			nextDe := de.next
			h := dict.hashKey(de.key) & dict.ht[1].sizeMask
			de.next = dict.ht[1].table[h]
			dict.ht[1].table[h] = de
			dict.ht[0].used--
			dict.ht[1].used++
			de = nextDe
		}
		dict.ht[0].table[dict.rehashIdx] = nil
		dict.rehashIdx++
	}

	if dict.ht[0].used == 0 {
		dict.ht[0] = dict.ht[1]
		dict.ht[1].reset()
		dict.rehashIdx = -1
	}
}

func (dict *Dict) size() int64 {
	return dict.ht[0].used + dict.ht[1].used
}
func (dict *Dict) find(key unsafe.Pointer) *dictEntry {
	if dict.size() == 0 {
		return nil
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}

	h := dict.hashKey(key)
	for table := 0; table < 2; table++ {
		idx := h & dict.ht[table].sizeMask
		he := dict.ht[table].table[idx]
		for he != nil {
			if he.key == key || dict.compareKey(key, he.key) {
				return he
			}
			he = he.next
		}
		if !dict.isRehashing() {
			return nil
		}
	}
	return nil
}

func SetHashFunctionSeed(seed []byte) {
	dictSeedKey = make([]byte, 16)
	copy(dictSeedKey, seed)
}

func GenCaseHashFunction(buf []byte) uint64 {
	h := siphash.New(dictSeedKey)
	h.Write(buf)
	return h.Sum64()
}

func (dict *Dict) Add(key, value unsafe.Pointer) bool {
	entry := dict.addRaw(key, nil)
	if entry == nil {
		return false
	}

	dict.setVal(entry, value)
	return true
}

func (dict *Dict) FetchValue(key unsafe.Pointer) unsafe.Pointer {
	he := dict.find(key)
	if he != nil {
		return dict.getVal(he)
	}
	return nil
}
