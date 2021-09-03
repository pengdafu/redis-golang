package ae

import (
	"syscall"

	"github.com/pengdafu/redis-golang/pkg/net"
	pkgTime "github.com/pengdafu/redis-golang/pkg/time"
)

// todo bugfix import circle
type apiState struct {
	Epfd   int
	Events []syscall.EpollEvent
}

func apiCreate(el *AeEventLoop) error {
	state := new(apiState)

	state.Events = make([]syscall.EpollEvent, el.SetSize, el.SetSize)
	epfd, err := syscall.EpollCreate(1024)
	if err != nil {
		return err
	}
	state.Epfd = epfd

	el.ApiData = state
	_ = net.AnetCloexec(epfd)
	return nil
}

func apiPoll(el *AeEventLoop, tvp *pkgTime.TimeVal) (numevents int) {
	state := el.ApiData.(*apiState)

	if tvp == nil {
		n, err := syscall.EpollWait(state.Epfd, state.Events, -1)
		if err != nil {
			return 0
		}
		numevents = n
	} else {
		n, err := syscall.EpollWait(state.Epfd, state.Events, int(tvp.Duration.Milliseconds()))
		if err != nil {
			return 0
		}
		numevents = n
	}

	if numevents > 0 {
		for i := 0; i < numevents; i++ {
			e := state.Events[i]
			mask := 0

			if e.Events&syscall.EPOLLIN != 0 {
				mask |= READABLE
			}
			if e.Events&syscall.EPOLLOUT != 0 {
				mask |= WRITEABLE
			}
			if e.Events&syscall.EPOLLERR != 0 {
				mask |= READABLE | WRITEABLE
			}
			if e.Events&syscall.EPOLLHUP != 0 {
				mask |= READABLE | WRITEABLE
			}

			el.Fired[i].Fd = uint64(e.Fd)
			el.Fired[i].Mask = mask
		}
	}
	return numevents
}
