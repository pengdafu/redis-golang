package ae

import (
	"github.com/pengdafu/redis-golang/pkg/net"
	"syscall"
)

type apiState struct {
	Epfd   int
	Events []*syscall.EpollEvent
}

func apiCreate(el *AeEventLoop) error {
	state := new(apiState)

	state.Events = make([]*syscall.EpollEvent, 0, el.SetSize)
	epfd, err := syscall.EpollCreate(1024)
	if err != nil {
		return err
	}
	state.Epfd = epfd

	el.ApiData = state
	_ = net.AnetCloexec(epfd)
	return nil
}
