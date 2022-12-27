//go:build darwin || freebsd

package ae

import (
	"github.com/pengdafu/redis-golang/anet"
	"github.com/pengdafu/redis-golang/util"
	"syscall"
	"time"
)

type aeApiState struct {
	KqFd   int
	Events []syscall.Kevent_t
}

func aeApiCreate(el *EventLoop) error {
	state := new(aeApiState)

	state.Events = make([]syscall.Kevent_t, el.SetSize)

	kqfd, err := syscall.Kqueue()
	if err != nil {
		return err
	}
	state.KqFd = kqfd

	_ = anet.Cloexec(kqfd)
	el.ApiData = state

	return nil
}

func aeApiPoll(el *EventLoop, tvp *util.TimeVal) (numevents int) {
	state := el.ApiData.(*aeApiState)

	if tvp == nil {
		n, err := syscall.Kevent(state.KqFd, nil, state.Events, nil)
		if err != nil {
			return 0
		}
		numevents = n
	} else {
		n, err := syscall.Kevent(state.KqFd, nil, state.Events, &syscall.Timespec{
			Sec:  int64(tvp.Duration / time.Second),
			Nsec: int64((tvp.Duration % time.Second) / time.Nanosecond),
		})
		if err != nil {
			return 0
		}
		numevents = n
	}

	if numevents > 0 {
		for i := 0; i < numevents; i++ {
			var mask int
			e := state.Events[i]

			if e.Filter&syscall.EVFILT_WRITE != 0 {
				mask |= Writeable
			}
			if e.Filter&syscall.EVFILT_READ != 0 {
				mask |= Readable
			}

			el.Fired[i].Fd = e.Ident
			el.Fired[i].Mask = mask
		}
	}
	return
}

func aeApiDelEvent(el *EventLoop, fd, mask int) {
	state := el.ApiData.(*aeApiState)

	ke := syscall.Kevent_t{}
	if mask&Readable != 0 {
		syscall.SetKevent(&ke, fd, syscall.EVFILT_READ, syscall.EV_DELETE)
		_, _ = syscall.Kevent(state.KqFd, []syscall.Kevent_t{ke}, nil, nil)
	}
	if mask&Writeable != 0 {
		syscall.SetKevent(&ke, fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
		_, _ = syscall.Kevent(state.KqFd, []syscall.Kevent_t{ke}, nil, nil)
	}
}

func aeApiAddEvent(el *EventLoop, fd, mask int) error {
	state := el.ApiData.(*aeApiState)

	ke := syscall.Kevent_t{}

	if mask&Readable != 0 {
		syscall.SetKevent(&ke, fd, syscall.EVFILT_READ, syscall.EV_ADD)
		if _, err := syscall.Kevent(state.KqFd, []syscall.Kevent_t{ke}, nil, nil); err != nil {
			return err
		}
	}
	if mask&Writeable != 0 {
		syscall.SetKevent(&ke, fd, syscall.EVFILT_WRITE, syscall.EV_ADD)
		if _, err := syscall.Kevent(state.KqFd, []syscall.Kevent_t{ke}, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func aeApiName() string {
	return "kqueue"
}

func aeApiFree(el *EventLoop) {
	state := el.ApiData.(*aeApiState)
	syscall.Close(state.KqFd)
}
