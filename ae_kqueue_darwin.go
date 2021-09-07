package main

import (
	"syscall"
	"time"
)

type aeApiState struct {
	KqFd   int
	Events []syscall.Kevent_t
}

func aeApiCreate(el *AeEventLoop) error {
	state := new(aeApiState)

	state.Events = make([]syscall.Kevent_t, el.SetSize)

	kqfd, err := syscall.Kqueue()
	if err != nil {
		return err
	}
	state.KqFd = kqfd

	_ = AnetCloexec(kqfd)
	el.ApiData = state

	return nil
}

func aeApiPoll(el *AeEventLoop, tvp *TimeVal) (numevents int) {
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
			Nsec: int64(tvp.Duration / time.Nanosecond),
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
				mask |= AE_WRITEABLE
			}
			if e.Filter&syscall.EVFILT_READ != 0 {
				mask |= AE_READABLE
			}

			el.Fired[i].Fd = e.Ident
			el.Fired[i].Mask = mask
		}
	}
	return
}

func aeApiDelEvent(el *AeEventLoop, fd, mask int) {
	state := el.ApiData.(*aeApiState)

	ke := syscall.Kevent_t{}
	if mask&AE_READABLE != 0 {
		syscall.SetKevent(&ke, fd, syscall.EVFILT_READ, syscall.EV_DELETE)
		_, _ = syscall.Kevent(state.KqFd, []syscall.Kevent_t{ke}, nil, nil)
	}
	if mask&AE_WRITEABLE != 0 {
		syscall.SetKevent(&ke, fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
		_, _ = syscall.Kevent(state.KqFd, []syscall.Kevent_t{ke}, nil, nil)
	}
}

func aeApiAddEvent(el *AeEventLoop, fd, mask int) error {
	state := el.ApiData.(*aeApiState)

	ke := syscall.Kevent_t{}

	if mask&AE_READABLE != 0 {
		syscall.SetKevent(&ke, fd, syscall.EVFILT_READ, syscall.EV_ADD)
		if _, err := syscall.Kevent(state.KqFd, []syscall.Kevent_t{ke}, nil, nil); err != nil {
			return err
		}
	}
	if mask&AE_WRITEABLE != 0 {
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
