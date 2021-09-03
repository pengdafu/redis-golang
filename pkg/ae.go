package pkg

import (
	"fmt"
)

const (
	AE_NONE      = 0
	AE_READABLE  = 1
	AE_WRITEABLE = 2
	AE_BARRIER   = 4
)

const (
	AE_FILE_EVENTS = 1 << iota
	AE_TIME_EVENTS
	AE_DONT_WAIT
	AE_CALL_BEFORE_SLEEP
	AE_CALL_AFTER_SLEEP
	AE_ALL_EVENTS = AE_FILE_EVENTS | AE_TIME_EVENTS
)

const (
	OK  = 0
	ERR = -1
)

type AeBeforeSleepProc func(eventLoop *AeEventLoop)
type AeEventLoop struct {
	MaxFd           int // 多路复用io一次最多返回多少个事件的fd
	SetSize         int
	TimeEventNextId int64
	Events          []fileEvent
	Fired           []firedEvent
	TimeEventHead   *timeEvent
	Stop            int
	ApiData         interface{} // todo
	BeforeSleep     AeBeforeSleepProc
	AfterSleep      AeBeforeSleepProc
	Flags           int
}

type AeFileProc func(el *AeEventLoop, fd int, clientData interface{}, mask int)
type fileEvent struct {
	Mask       int // AE_(AE_WRITEABLE|AE_READABLE|AE_BARRIER) 中的一种
	RFileProc  AeFileProc
	WFileProc  AeFileProc
	ClientData interface{}
}

type firedEvent struct {
	Fd   uint64
	Mask int
}

type AeTimeProc func(eventLoop *AeEventLoop, id uint64, clientData interface{}) int
type aeEventFinalizerProc func(eventLoop *AeEventLoop, clientData interface{})
type timeEvent struct {
	Id            int64
	When          int64 // timestamp
	TimeProc      AeTimeProc
	FinalizerProc aeEventFinalizerProc
	ClientData    interface{}
	Prev          *timeEvent
	Next          *timeEvent
	RefCount      int // 在递归时间事件被调用时，refCount为了防止时间器事件被释放
}

func AeCreateEventLoop(setsize int) (*AeEventLoop, error) {
	el := new(AeEventLoop)

	el.Events = make([]fileEvent, setsize, setsize)
	el.Fired = make([]firedEvent, setsize, setsize)
	el.SetSize = setsize
	el.TimeEventHead = nil
	el.TimeEventNextId = 0
	el.MaxFd = -1
	el.Stop = 0
	el.BeforeSleep = nil
	el.AfterSleep = nil
	el.Flags = 0

	// 创建多路复用
	if err := aeApiCreate(el); err != nil {
		return nil, err
	}

	for i := 0; i < setsize; i++ {
		el.Events[i].Mask = AE_NONE
	}

	return el, nil
}

func (el *AeEventLoop) AeApiPoll(tvp *TimeVal) int {
	return aeApiPoll(el, tvp)
}

func (el *AeEventLoop) AeCreateTimeEvent(milliseconds int64, proc AeTimeProc, clientData interface{}, finalizerProc aeEventFinalizerProc) int64 {
	id := el.TimeEventNextId
	el.TimeEventNextId++

	te := new(timeEvent)
	te.Id = id
	te.When = GetMonotonicUs() + milliseconds*1000
	te.TimeProc = proc
	te.FinalizerProc = finalizerProc
	te.ClientData = clientData
	te.Prev = nil
	te.Next = el.TimeEventHead
	te.RefCount = 0

	if te.Next != nil {
		te.Next.Prev = te
	}
	el.TimeEventHead = te

	return id
}

func (el *AeEventLoop) AeDeleteFileEvent(fd, mask int) {
	if fd >= el.SetSize {
		return
	}

	fe := el.Events[fd]
	if fe.Mask == AE_NONE {
		return
	}

	if mask&AE_WRITEABLE != 0 {
		mask |= AE_BARRIER
	}

	aeApiDelEvent(el, fd, mask)
	fe.Mask = fe.Mask & (^mask)
	if fd == el.MaxFd && fe.Mask == AE_NONE {
		for i := el.MaxFd - 1; i >= 0; i-- {
			if el.Events[i].Mask != AE_NONE {
				el.MaxFd = i
				return
			}
		}
	}
}

func (el *AeEventLoop) AeCreateFileEvent(fd, mask int, proc AeFileProc, clientData interface{}) error {
	if fd >= el.SetSize {
		return fmt.Errorf("fd out of range: %d", fd)
	}

	fe := &el.Events[fd]
	if err := aeApiAddEvent(el, fd, mask); err != nil {
		return fmt.Errorf("aeApiAddEvent err: %v", err)
	}

	fe.Mask |= mask
	if mask&AE_WRITEABLE != 0 {
		fe.WFileProc = proc
	}
	if mask&AE_READABLE != 0 {
		fe.RFileProc = proc
	}
	fe.ClientData = clientData
	if fd > el.MaxFd {
		el.MaxFd = fd
	}
	return nil
}
