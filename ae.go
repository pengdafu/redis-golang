package main

import (
	"fmt"
	"time"
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

func aeCreateEventLoop(setsize int) (*AeEventLoop, error) {
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

func (el *AeEventLoop) aeApiPoll(tvp *TimeVal) int {
	return aeApiPoll(el, tvp)
}

func (el *AeEventLoop) aeCreateTimeEvent(milliseconds int64, proc AeTimeProc, clientData interface{}, finalizerProc aeEventFinalizerProc) int64 {
	id := el.TimeEventNextId
	el.TimeEventNextId++

	te := new(timeEvent)
	te.Id = id
	te.When = getMonotonicUs() + milliseconds*1000
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

func (el *AeEventLoop) aeDeleteFileEvent(fd, mask int) {
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

func (el *AeEventLoop) aeCreateFileEvent(fd, mask int, proc AeFileProc, clientData interface{}) error {
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

func aeMain(el *AeEventLoop) {
	el.Stop = 0
	for el.Stop == 0 {
		aeProcessEvent(el, AE_ALL_EVENTS|AE_CALL_BEFORE_SLEEP|AE_CALL_AFTER_SLEEP)
	}
}

func aeProcessEvent(el *AeEventLoop, flags int) (processed int) {
	var numevents int

	// 没有时间事件和文件(IO)事件，什么也不处理
	if flags&AE_TIME_EVENTS == 0 && flags&AE_FILE_EVENTS == 0 {
		return
	}

	// 注意，只要我们处理时间事件以便于休眠到下一个时间事件触发，我们也应该要处理select() events，哪怕没有file events
	// 要处理
	if el.MaxFd != -1 || (flags&AE_TIME_EVENTS != 0 && flags&AE_DONT_WAIT == 0) {
		timeVal := &TimeVal{}
		msUntilTimer := int64(-1)

		if flags&AE_TIME_EVENTS != 0 && flags&AE_DONT_WAIT == 0 {
			msUntilTimer = msUntilEarliestTimer(el)
		}

		if msUntilTimer > 0 {
			timeVal.Duration = time.Millisecond * time.Duration(msUntilTimer)
		} else {
			if flags&AE_DONT_WAIT != 0 {
				//timeVal.Duration = time.Second * 0
			} else {
				timeVal = nil
			}
		}

		if el.Flags&AE_DONT_WAIT != 0 {
			timeVal = &TimeVal{}
		}

		if el.BeforeSleep != nil && flags&AE_CALL_BEFORE_SLEEP != 0 {
			el.BeforeSleep(el)
		}

		numevents = el.aeApiPoll(timeVal)

		if el.AfterSleep != nil && flags&AE_CALL_AFTER_SLEEP != 0 {
			el.AfterSleep(el)
		}

		for i := 0; i < numevents; i++ {
			fe := el.Events[el.Fired[i].Fd]
			fd := el.Fired[i].Fd
			mask := el.Fired[i].Mask
			fired := 0

			invert := fe.Mask & AE_BARRIER

			if invert == 0 && fe.Mask&mask&AE_READABLE != 0 {
				fe.RFileProc(el, int(fd), fe.ClientData, mask)
				fired++
				fe = el.Events[fd]
			}

			processed++
		}
	}

	if flags&AE_TIME_EVENTS != 0 {
		processed += processTimeEvents(el)
	}

	return processed
}

func processTimeEvents(el *AeEventLoop) int {
	return 0
}

// msUntilEarliestTimer 返回距离第一个定时器被触发的时间还剩多少ms
// 如果没有定时器，返回-1
// 注意，time event 没有排序，获取的时间复杂度为O(N)
// 可能的优化点(Redis 暂时还不需要，但是...):
//  1.插入事件的时候就排序，这样最近的时间事件就是head，这样虽然会更好，但是插入和删除变成了O(N)
// 	2.使用跳表，这样获取变成了O(1)并且插入是O(log(N))
func msUntilEarliestTimer(el *AeEventLoop) int64 {
	if el.TimeEventHead == nil {
		return -1
	}

	earliest := el.TimeEventHead
	te := el.TimeEventHead
	for te != nil {
		if te.When < earliest.When {
			earliest = te
		}
		te = te.Next
	}
	now := getMonotonicUs()
	if now >= earliest.When {
		return 0
	}
	return (earliest.When - now) / 1000
}
