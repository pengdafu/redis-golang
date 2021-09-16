package main

type listNode struct {
	prev  *listNode
	next  *listNode
	value interface{}
}

type list struct {
	head, tail *listNode
	dup        func(interface{}) interface{}
	free       func(interface{})
	match      func(ptr interface{}, key interface{}) int
	len        uint64
}

func listCreate() *list {
	return new(list)
}

func listSetFreeMethod(l *list, fn func(interface{})) {
	l.free = fn
}

func listSetMatchMethod(l *list, fn func(interface{}, interface{}) int) {
	l.match = fn
}

func listSetDupMethod(l *list, fn func(interface{}) interface{}) {
	l.dup = fn
}
