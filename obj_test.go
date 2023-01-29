package main

import (
	"fmt"
	"testing"
)

func TestObject(t *testing.T) {
	server = &RedisServer{hz: 1}
	o := createObject(ObjString, 10)
	en1 := o.getEncoding()
	o.setEncoding(ObjEncodingInt)
	en2 := o.getEncoding()
	change(o)
	en3 := o.getEncoding()
	fmt.Println(en1, en2, en3)
}

func change(o *robj) {
	o.setEncoding(ObjEncodingHt)
}
