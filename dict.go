package main

type dict struct {
}

type dictType interface {
}

func dictCreate(tp dictType, privData interface{}) *dict {
	return new(dict)
}
