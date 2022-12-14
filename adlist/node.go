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
