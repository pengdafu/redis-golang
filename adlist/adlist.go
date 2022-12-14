package adlist

type List struct {
	head, tail *ListNode
	dup        func(interface{}) interface{}
	free       func(interface{})
	match      func(ptr interface{}, key interface{}) int
	len        int
}

func Create() *List {
	return new(List)
}

func (l *List) SetFreeMethod(fn func(interface{})) {
	l.free = fn
}

func (l *List) SetMatchMethod(fn func(interface{}, interface{}) int) {
	l.match = fn
}

func (l *List) SetDupMethod(fn func(interface{}) interface{}) {
	l.dup = fn
}

func (l *List) Len() int {
	return l.len
}

func (l *List) AddNodeHead(value interface{}) *List {
	node := &ListNode{
		prev:  nil,
		next:  nil,
		value: value,
	}
	if l.len == 0 {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}

	l.len++
	return l
}

func (l *List) AddNodeTail(value interface{}) *List {
	node := &ListNode{
		prev:  nil,
		next:  nil,
		value: value,
	}
	if l.len == 0 {
		l.head = node
		l.tail = node
	} else {
		node.prev = l.tail
		l.tail.next = node
		l.tail = node
	}

	l.len++
	return l
}

func (l *List) Last() *ListNode {
	return l.tail
}
