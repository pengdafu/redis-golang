package logic

import "github.com/pengdafu/redis-golang/pkg/ae"

const (
	MAX_ACCEPTS_PER_CALL = 1000
)

func AcceptTcpHandler(el *ae.AeEventLoop, fd int, privdate interface{}, mask int) {
	max := MAX_ACCEPTS_PER_CALL
	for max > 0 {
		max--

	}
}
