package dict

type Iterator struct {
	d                *Dict
	index            int64
	table            int
	safe             bool
	entry, nextEntry *Entry
	fingerprint      int64
}

func (iter *Iterator) Next() *Entry {
	for {
		if iter.entry == nil {
			ht := &iter.d.ht[iter.table]
			if iter.index == -1 && iter.table == 0 {
				if iter.safe {
					iter.d.iterators++
				} else {
					iter.fingerprint = iter.d.fingerprint()
				}
			}
			iter.index++
			if iter.index >= ht.size {
				if iter.d.IsRehashing() && iter.table == 0 {
					iter.table++
					iter.index = 0
					ht = &iter.d.ht[1]
				} else {
					break
				}
			}
			iter.entry = ht.table[iter.index]
		} else {
			iter.entry = iter.nextEntry
		}
		if iter.entry != nil {
			iter.nextEntry = iter.entry.next
			return iter.entry
		}
	}
	return nil
}

func (iter *Iterator) Release() {
	if !(iter.index == -1 && iter.table == 0) {
		if iter.safe {
			iter.d.iterators--
		}
	}
}
