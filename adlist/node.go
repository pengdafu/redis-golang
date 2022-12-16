package adlist

type ListNode struct {
	prev  *ListNode
	next  *ListNode
	value interface{}
}

func (n *ListNode) NodeValue() interface{} {
	if n == nil {
		return nil
	}
	return n.value
}

const (
	alStartHead = 0
	alStartTail = 1
)

type ListIter struct {
	next      *ListNode
	direction int
}

func (iter *ListIter) Next() *ListNode {
	cur := iter.next
	if cur != nil {
		if iter.direction == alStartHead {
			iter.next = cur.next
		} else {
			iter.next = cur.prev
		}
	}
	return cur
}
