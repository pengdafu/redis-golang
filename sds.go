package main

type sds []byte

func sdsempty() sds {
	return sdsnewlen("", 0)
}

func sdsnewlen(init string, initlen int) sds {
	return nil
}
