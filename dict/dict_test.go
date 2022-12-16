package dict

import (
	"fmt"
	"github.com/pengdafu/redis-golang/util"
	"testing"
	"unsafe"
)

var typ = &Type{
	HashFunction: func(key unsafe.Pointer) uint64 {
		return GenCaseHashFunction(*(*[]byte)(key))
	},
	KeyDup: nil,
	ValDup: nil,
	KeyCompare: func(privData interface{}, key1, key2 unsafe.Pointer) bool {
		return util.BytesCmp(*(*[]byte)(key1), *(*[]byte)(key2))
	},
	KeyDestructor: nil,
	ValDestructor: nil,
}

func TestCreate(t *testing.T) {
	SetHashFunctionSeed(util.GetRandomBytes(16))
	d := Create(typ, nil)
	keys := make([][]byte, 30)
	for i := 0; i < 30; i++ {
		if i == 20 {
			fmt.Println()
		}
		keys[i] = []byte(fmt.Sprintf("key%d", i))
		d.Add(unsafe.Pointer(&keys[i]), unsafe.Pointer(&keys[i]))
	}

	for i := 0; i < 30; i++ {
		fmt.Println(string(*(*[]byte)(d.FetchValue(unsafe.Pointer(&keys[i])))))
	}
}
