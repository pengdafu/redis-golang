//go:build linux

package ae

import (
	"github.com/pengdafu/redis-golang/anet"
	"github.com/pengdafu/redis-golang/util"
	"syscall"
)

type aeApiState struct {
	Epfd   int
	Events []syscall.EpollEvent
}

func aeApiCreate(el *EventLoop) error {
	state := new(aeApiState)

	state.Events = make([]syscall.EpollEvent, el.SetSize)
	epfd, err := syscall.EpollCreate(1024)
	if err != nil {
		return err
	}
	state.Epfd = epfd

	el.ApiData = state
	_ = anet.Cloexec(epfd)
	return nil
}

func aeApiPoll(el *EventLoop, tvp *util.TimeVal) (numevents int) {
	state := el.ApiData.(*aeApiState)

	if tvp == nil {
		n, err := syscall.EpollWait(state.Epfd, state.Events, -1)
		if err != nil {
			return 0
		}
		numevents = n
	} else {
		n, err := syscall.EpollWait(state.Epfd, state.Events, tvp.MillionSeconds())
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
				mask |= Readable
			}
			if e.Events&syscall.EPOLLOUT != 0 {
				mask |= Writeable
			}
			if e.Events&syscall.EPOLLERR != 0 {
				mask |= Readable | Writeable
			}
			if e.Events&syscall.EPOLLHUP != 0 {
				mask |= Readable | Writeable
			}

			el.Fired[i].Fd = uint64(e.Fd)
			el.Fired[i].Mask = mask
		}
	}
	return numevents
}

func aeApiAddEvent(el *EventLoop, fd, mask int) error {
	state := el.ApiData.(*aeApiState)
	ee := &syscall.EpollEvent{}
	op := 0
	if el.Events[fd].Mask&mask != None {
		op = syscall.EPOLL_CTL_MOD
	} else {
		op = syscall.EPOLL_CTL_ADD
	}
	mask |= el.Events[fd].Mask
	if mask&Readable != 0 {
		ee.Events |= syscall.EPOLLIN
	}
	if mask&Writeable != 0 {
		ee.Events |= syscall.EPOLLOUT
	}
	ee.Fd = int32(fd)

	return syscall.EpollCtl(state.Epfd, op, fd, ee)
}

func aeApiDelEvent(el *EventLoop, fd, delmask int) {
	state := el.ApiData.(*aeApiState)
	ee := &syscall.EpollEvent{}
	mask := el.Events[fd].Mask & (^delmask)

	if mask&Readable != 0 {
		ee.Events |= syscall.EPOLLIN
	}
	if mask&Writeable != 0 {
		ee.Events |= syscall.EPOLLOUT
	}

	ee.Fd = int32(fd)
	if mask != None {
		_ = syscall.EpollCtl(state.Epfd, syscall.EPOLL_CTL_MOD, fd, ee)
	} else {
		_ = syscall.EpollCtl(state.Epfd, syscall.EPOLL_CTL_DEL, fd, ee)
	}
}

func aeApiName() string {
	return "epoll"
}

func aeApiFree(el *EventLoop) {
	state := el.ApiData.(*aeApiState)
	syscall.Close(state.Epfd)
}
