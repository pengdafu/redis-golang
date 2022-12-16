package dict

type dictHt struct {
	table      []*dictEntry
	size, used int64
	sizeMask   uint64
}

func (ht *dictHt) reset() {
	ht.table = nil
	ht.size = 0
	ht.sizeMask = 0
	ht.used = 0
}
