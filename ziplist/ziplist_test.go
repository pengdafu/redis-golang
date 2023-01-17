package ziplist

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	zl := New()
	fmt.Println(*ziplistLength(zl), *ziplistBytes(zl), *ziplistTailOffset(zl))

	zl = Push(zl, []byte("pdf"), Tail)
	zl = Push(zl, []byte("pdf1"), Tail)
	zl = Push(zl, []byte("pdf2"), Tail)
	h := Index(zl, Head)
	k := Find(h, []byte("pdf1"), 4, 0)
	//n := Next(zl, k)
	zl = Delete(zl, &k)

	zl = Insert(zl, k, []byte("ghj12"))
	fmt.Println(*ziplistLength(zl))
	fmt.Println(*ziplistLength(zl), *ziplistBytes(zl), *ziplistTailOffset(zl))
	fmt.Println(string(zl))
}
