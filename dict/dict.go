package dict

import (
	"bytes"
	"encoding/binary"
	"github.com/pengdafu/redis-golang/util"
	"math"
	"unsafe"
)

var dictSeedKey []byte

var (
	dictCanResize        = true
	dictForceResizeRatio = int64(5)
	HtInitialSize        = int64(4)
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
func (dict *Dict) addRaw(key unsafe.Pointer, existing **Entry) *Entry {
	if dict.IsRehashing() {
		dict.rehashStep()
	}

	var index int
	if index = dict.keyIndex(key, dict.hashKey(key), existing); index == -1 {
		return nil
	}

	ht := &dict.ht[0]
	if dict.IsRehashing() {
		ht = &dict.ht[1]
	}
	entry := new(Entry)
	entry.next = ht.table[index]
	ht.table[index] = entry
	ht.used++

	dict.setKey(entry, key)
	return entry
}

func (dict *Dict) setKey(entry *Entry, key unsafe.Pointer) {
	if dict.typ.KeyDup != nil {
		entry.key = dict.typ.KeyDup(dict.privData, key)
	} else {
		entry.key = key
	}
}

func (dict *Dict) IsRehashing() bool {
	return dict.rehashIdx != -1
}

func (dict *Dict) expand(size int64) bool {
	if dict.IsRehashing() || dict.ht[0].used > size {
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
	n.table = make([]*Entry, realSize)
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
	i := HtInitialSize
	for {
		if i >= size {
			return i
		}
		i *= 2
	}
}

func (dict *Dict) expandIfNeeded() bool {
	if dict.IsRehashing() {
		return true
	}

	if dict.ht[0].size == 0 {
		return dict.expand(HtInitialSize)
	}

	if dict.ht[0].used >= dict.ht[0].size && (dictCanResize || dict.ht[0].used/dict.ht[0].size >= dictForceResizeRatio) {
		return dict.expand(dict.ht[0].used * 2)
	}
	return true
}

func (dict *Dict) keyIndex(key unsafe.Pointer, hash uint64, existing **Entry) int {
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
		if !dict.IsRehashing() {
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

func (dict *Dict) rehash(n int) bool {
	emptyVisit := 10 * n
	if !dict.IsRehashing() {
		return false
	}

	for ; n > 0 && dict.ht[0].used != 0; n-- {
		for dict.ht[0].table[dict.rehashIdx] == nil {
			dict.rehashIdx++
			emptyVisit--
			if emptyVisit == 0 {
				return true
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
	return true
}

func (dict *Dict) Size() int64 {
	return dict.ht[0].used + dict.ht[1].used
}

func (dict *Dict) Slots() int64 {
	return dict.ht[0].size + dict.ht[1].size
}

func SetHashFunctionSeed(seed []byte) {
	dictSeedKey = make([]byte, 16)
	copy(dictSeedKey, seed)
}

func GenCaseHashFunction(buf []byte) uint64 {
	return siphash_nocase(dictSeedKey, buf)
}

func GenHashFunction(buf []byte) uint64 {
	return siphash(dictSeedKey, buf)
}

/* SipHash-2-4 */
// siphash is a function that takes in a key and a message and returns a 64-bit hash
func siphash(key []byte, message []byte) uint64 {
	k0 := binary.LittleEndian.Uint64(key[:8])
	k1 := binary.LittleEndian.Uint64(key[8:])

	b := uint64(len(message)) << 56
	v0 := k0 ^ 0x736f6d6570736575
	v1 := k1 ^ 0x646f72616e646f6d
	v2 := k0 ^ 0x6c7967656e657261
	v3 := k1 ^ 0x7465646279746573
	tmpMessage := message
	for len(tmpMessage) >= 8 {
		m := binary.LittleEndian.Uint64(tmpMessage)
		v3 ^= m
		sipRound(&v0, &v1, &v2, &v3)
		sipRound(&v0, &v1, &v2, &v3)
		v0 ^= m
		tmpMessage = tmpMessage[8:]
	}

	m := b | uint64(message[0])<<48
	switch len(message) {
	case 7:
		m |= uint64(message[6]) << 48
		fallthrough
	case 6:
		m |= uint64(message[5]) << 40
		fallthrough
	case 5:
		m |= uint64(message[4]) << 32
		fallthrough
	case 4:
		m |= uint64(message[3]) << 24
		fallthrough
	case 3:
		m |= uint64(message[2]) << 16
		fallthrough
	case 2:
		m |= uint64(message[1]) << 8
		fallthrough
	case 1:
		m |= uint64(message[0])
	}

	v3 ^= m
	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)
	v0 ^= m
	v2 ^= 0xff

	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)

	return v0 ^ v1 ^ v2 ^ v3
}

// sipRound is a helper function that performs one round of SipHash-2-4
// sipRound is a helper function that performs one round of SipHash-2-4
func sipRound(v0, v1, v2, v3 *uint64) {
	*v0 += *v1
	*v1 = (*v1 << 13) | (*v1 >> 51)
	*v2 += *v3
	*v3 = (*v3 << 16) | (*v3 >> 48)
	*v1 ^= *v0
	*v3 ^= *v2
	*v0 = (*v0 << 32) | (*v0 >> 32)
	*v2 += *v1
	*v1 = (*v1 << 17) | (*v1 >> 47)
	*v3 += *v0
	*v0 = (*v0 << 21) | (*v0 >> 43)
	*v2 ^= *v1
	*v1 = (*v1 << 32) | (*v1 >> 32)
}

// siphash_nocase is a function that takes in a key and a message and returns a 64-bit hash,
// ignoring the case of the message
func siphash_nocase(key []byte, message []byte) uint64 {
	// Convert message to lowercase
	lowerMessage := make([]byte, len(message))
	for i, b := range message {
		lowerMessage[i] = bytes.ToLower([]byte{b})[0]
	}

	// Return hash of lowercase message
	return siphash(key, lowerMessage)
}

func (dict *Dict) Add(key, value unsafe.Pointer) bool {
	entry := dict.addRaw(key, nil)
	if entry == nil {
		return false
	}

	dict.SetVal(entry, value)
	return true
}

func (dict *Dict) FetchValue(key unsafe.Pointer) unsafe.Pointer {
	he := dict.Find(key)
	if he != nil {
		return dict.GetVal(he)
	}
	return nil
}
func (dict *Dict) Find(key unsafe.Pointer) *Entry {
	if dict.Size() == 0 {
		return nil
	}
	if dict.IsRehashing() {
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
		if !dict.IsRehashing() {
			return nil
		}
	}
	return nil
}
func (dict *Dict) SetVal(entry *Entry, obj unsafe.Pointer) {
	if dict.typ.ValDup != nil {
		entry.v.val = dict.typ.ValDup(dict.privData, obj)
	} else {
		entry.v.val = obj
	}
}
func (dict *Dict) GetVal(entry *Entry) unsafe.Pointer {
	return entry.v.val
}
func SetSignedIntegerVal(entry *Entry, val int64) {
	entry.v.s64 = val
}
func GetSignedIntegerVal(enter *Entry) int64 {
	return enter.v.s64
}
func GetKey(e *Entry) unsafe.Pointer {
	return e.key
}

func (dict *Dict) Delete(key unsafe.Pointer) bool {
	return dict.GenericDelete(key, false) != nil
}

func (dict *Dict) GenericDelete(key unsafe.Pointer, nofree bool) *Entry {
	if dict.ht[0].used == 0 && dict.ht[1].used == 0 {
		return nil
	}

	if dict.IsRehashing() {
		dict.rehashStep()
	}

	h := dict.hashKey(key)
	var prevHe *Entry
	for table := 0; table < 2; table++ {
		idx := h & dict.ht[table].sizeMask
		he := dict.ht[table].table[idx]
		prevHe = nil
		for he != nil {
			if key == he.key || dict.compareKey(key, he.key) {
				if prevHe != nil {
					prevHe.next = he.next
				} else {
					dict.ht[table].table[idx] = he.next
				}
				if !nofree {

				}
				dict.ht[table].used--
				return he
			}
			prevHe = he
			he = he.next
		}
		if !dict.IsRehashing() {
			break
		}
	}
	return nil
}

func (dict *Dict) AddOrFind(key unsafe.Pointer) *Entry {
	existing := &Entry{}
	entry := dict.addRaw(key, &existing)
	if entry != nil {
		return entry
	}
	return existing
}

func (dict *Dict) Resize() bool {
	if !dictCanResize || dict.IsRehashing() {
		return false
	}
	minimal := dict.ht[0].used
	if minimal < HtInitialSize {
		minimal = HtInitialSize
	}
	return dict.expand(minimal)
}

func (dict *Dict) RehashMilliseconds(ms int) int {
	start := util.GetMillionSeconds()

	rehashes := 0
	for dict.rehash(100) {
		rehashes += 100
		if (util.GetMillionSeconds() - start) > int64(ms) {
			break
		}
	}
	return rehashes
}

func (dict *Dict) SizeMask(table int) uint64 {
	return dict.ht[table].sizeMask
}

func (dict *Dict) DictEntry(table int, idx uint64) *Entry {
	return dict.ht[table].table[idx]
}
