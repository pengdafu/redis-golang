package main

import (
	"fmt"
	"testing"
)

func TestObject(t *testing.T) {
	server = &RedisServer{}
	o := createObject(ObjString, 10)
	en1 := o.getEncoding()
	o.setEncoding(ObjEncodingInt)
	en2 := o.getEncoding()

	fmt.Println(en1, en2)
}
