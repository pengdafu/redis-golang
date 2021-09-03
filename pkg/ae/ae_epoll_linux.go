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

func apiAddEvent(el *AeEventLoop, fd, mask int) error {
	state := el.ApiData.(*apiState)
	ee := &syscall.EpollEvent{}
	op := 0
	if el.Events[fd].Mask&mask != NONE {
		op = syscall.EPOLL_CTL_MOD
	} else {
		op = syscall.EPOLL_CTL_MOD
	}
	mask |= el.Events[fd].Mask
	if mask&READABLE != 0 {
		ee.Events |= syscall.EPOLLIN
	}
	if mask&WRITEABLE != 0 {
		ee.Events |= syscall.EPOLLOUT
	}
	ee.Fd = int32(fd)

	return syscall.EpollCtl(state.Epfd, op, fd, ee)
}

func apiDelEvent(el *AeEventLoop, fd, delmask int) {
	state := el.ApiData.(*apiState)
	ee := &syscall.EpollEvent{}
	mask := el.Events[fd].Mask & (^delmask)

	if mask&READABLE != 0 {
		ee.Events |= syscall.EPOLLIN
	}
	if mask&WRITEABLE != 0 {
		ee.Events |= syscall.EPOLLOUT
	}

	ee.Fd = int32(fd)
	if mask != NONE {
		_ = syscall.EpollCtl(state.Epfd, syscall.EPOLL_CTL_MOD, fd, ee)
	} else {
		_ = syscall.EpollCtl(state.Epfd, syscall.EPOLL_CTL_DEL, fd, ee)
	}
}

func apiName() string {
	return "epoll"
}
