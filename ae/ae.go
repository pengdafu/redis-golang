package ae

import (
	"fmt"
	"github.com/pengdafu/redis-golang/util"
	"log"
	"os"
	"runtime/debug"
	"time"
)

const (
	None      = 0
	Readable  = 1
	Writeable = 2
	Barrier   = 4
)

const (
	FileEvents = 1 << iota
	TimeEvents
	DontWait
	CallBeforeSleep
	CallAfterSleep
	AllEvents = FileEvents | TimeEvents
)

const (
	OK  = 0
	ERR = -1
	DeletedEventId
	NoMore
)

type BeforeSleepProc func(eventLoop *EventLoop)
type EventLoop struct {
	MaxFd           int // 多路复用io一次最多返回多少个事件的fd
	SetSize         int
	TimeEventNextId uint64
	LastTime        time.Time
	Events          []fileEvent
	Fired           []firedEvent
	TimeEventHead   *timeEvent
	Stop            int
	ApiData         interface{} // todo
	BeforeSleep     BeforeSleepProc
	AfterSleep      BeforeSleepProc
	Flags           int
}

type FileProc func(el *EventLoop, fd int, clientData interface{}, mask int)
type fileEvent struct {
	Mask       int // AE_(Writeable|AE_READABLE|AE_BARRIER) 中的一种
	RFileProc  FileProc
	WFileProc  FileProc
	ClientData interface{}
}

type firedEvent struct {
	Fd   uint64
	Mask int
}

type TimeProc func(eventLoop *EventLoop, id int64, clientData interface{}) int
type aeEventFinalizerProc func(eventLoop *EventLoop, clientData interface{})
type timeEvent struct {
	Id            int64
	WhenTimestamp int64 // timestamp
	TimeProc      TimeProc
	FinalizerProc aeEventFinalizerProc
	ClientData    interface{}
	Prev          *timeEvent
	Next          *timeEvent
	RefCount      int // 在递归时间事件被调用时，refCount为了防止时间器事件被释放
}

func CreateEventLoop(setsize int) (*EventLoop, error) {
	el := new(EventLoop)

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
		el.Events[i].Mask = None
	}

	return el, nil
}

func (el *EventLoop) aeApiPoll(tvp *util.TimeVal) int {
	return aeApiPoll(el, tvp)
}

func (el *EventLoop) CreateTimeEvent(milliseconds int64, proc TimeProc, clientData interface{}, finalizerProc aeEventFinalizerProc) int64 {
	id := el.TimeEventNextId
	el.TimeEventNextId++

	te := new(timeEvent)
	te.Id = int64(id)
	te.WhenTimestamp = util.GetMillionSeconds() + milliseconds
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

	return te.Id
}

func (el *EventLoop) AeDeleteFileEvent(fd, mask int) {
	if fd >= el.SetSize {
		return
	}

	fe := el.Events[fd]
	if fe.Mask == None {
		return
	}

	if mask&Writeable != 0 {
		mask |= Barrier
	}

	aeApiDelEvent(el, fd, mask)
	fe.Mask = fe.Mask & (^mask)
	if fd == el.MaxFd && fe.Mask == None {
		for i := el.MaxFd - 1; i >= 0; i-- {
			if el.Events[i].Mask != None {
				el.MaxFd = i
				return
			}
		}
	}
}

func (el *EventLoop) AeCreateFileEvent(fd, mask int, proc FileProc, clientData interface{}) error {
	if fd >= el.SetSize {
		return fmt.Errorf("fd out of range: %d", fd)
	}

	fe := &el.Events[fd]
	if err := aeApiAddEvent(el, fd, mask); err != nil {
		return fmt.Errorf("aeApiAddEvent err: %v", err)
	}

	fe.Mask |= mask
	if mask&Writeable != 0 {
		fe.WFileProc = proc
	}
	if mask&Readable != 0 {
		fe.RFileProc = proc
	}
	fe.ClientData = clientData
	if fd > el.MaxFd {
		el.MaxFd = fd
	}
	return nil
}

func (el *EventLoop) AeSetBeforeSleepProc(beforeSleep BeforeSleepProc) {
	el.BeforeSleep = beforeSleep
}

func (el *EventLoop) AeMain() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("recovery err: ", err)
			debug.PrintStack()
			os.Exit(1)
		}
	}()
	el.Stop = 0
	for el.Stop == 0 {
		aeProcessEvent(el, AllEvents|CallBeforeSleep|CallAfterSleep)
	}
}

func (el *EventLoop) AeDeleteEventLoop() {
	el.Stop = 1
	aeApiFree(el)
}

func aeProcessEvent(el *EventLoop, flags int) (processed int) {
	var numevents int

	// 没有时间事件和文件(IO)事件，什么也不处理
	if flags&TimeEvents == 0 && flags&FileEvents == 0 {
		return
	}

	// 注意，只要我们处理时间事件以便于休眠到下一个时间事件触发，我们也应该要处理select() events，哪怕没有file events
	// 要处理
	if el.MaxFd != -1 || (flags&TimeEvents != 0 && flags&DontWait == 0) {
		tvp := &util.TimeVal{}
		var shortest *timeEvent

		if flags&TimeEvents != 0 && flags&DontWait == 0 {
			shortest = aeSearchNearestTimer(el)
		}

		if shortest != nil {
			now := util.GetMillionSeconds()
			ms := shortest.WhenTimestamp - now

			if ms > 0 {
				tvp.Sec = ms / 1000
				tvp.Usec = ms % 1000 * 1000
			} else {
				tvp.Sec = 0
				tvp.Usec = 0
			}
		} else {
			if flags&DontWait > 0 {
				tvp.Sec = 0
				tvp.Usec = 0
			} else {
				tvp = nil // wait forever
			}
		}

		if el.Flags&DontWait != 0 {
			tvp.Sec = 0
			tvp.Usec = 0
		}

		if el.BeforeSleep != nil && flags&CallBeforeSleep != 0 {
			el.BeforeSleep(el)
		}

		numevents = el.aeApiPoll(tvp)

		if el.AfterSleep != nil && flags&CallAfterSleep != 0 {
			el.AfterSleep(el)
		}

		for i := 0; i < numevents; i++ {
			fe := el.Events[el.Fired[i].Fd]
			fd := el.Fired[i].Fd
			mask := el.Fired[i].Mask
			fired := 0

			invert := fe.Mask & Barrier

			if invert == 0 && fe.Mask&mask&Readable != 0 {
				fe.RFileProc(el, int(fd), fe.ClientData, mask)
				fired++
				fe = el.Events[fd]
			}

			processed++
		}
	}

	if flags&TimeEvents != 0 {
		processed += processTimeEvents(el)
	}

	return processed
}

func processTimeEvents(el *EventLoop) int {
	var processed int
	var te *timeEvent

	now := time.Now()
	if now.Before(el.LastTime) {
		te = el.TimeEventHead
		for te != nil {
			te.WhenTimestamp = 0
			te = te.Next
		}
	}
	el.LastTime = now

	te = el.TimeEventHead
	maxId := el.TimeEventNextId - 1
	for te != nil {
		if te.Id == DeletedEventId {
			next := te.Next
			if te.RefCount > 0 {
				te = next
				continue
			}
			if te.Prev != nil {
				te.Prev.Next = te.Next
			} else {
				el.TimeEventHead = te.Next
			}

			if te.Next != nil {
				te.Next.Prev = te.Prev
			}
			if te.FinalizerProc != nil {
				te.FinalizerProc(el, te.ClientData)
			}
			te = next
			continue
		}

		if te.Id > int64(maxId) {
			te = te.Next
			continue
		}

		if now.UnixMilli() > te.WhenTimestamp {
			id := te.Id
			te.RefCount++
			retVal := te.TimeProc(el, id, te.ClientData)
			te.RefCount--
			processed++
			if retVal != NoMore {
				te.WhenTimestamp = now.UnixMilli() + int64(retVal)
			} else {
				te.Id = DeletedEventId
			}
		}
		te = te.Next
	}
	return processed
}

// aeSearchNearestTimer 返回距离第一个定时器被触发的时间还剩多少ms
// 如果没有定时器，返回-1
// 注意，time event 没有排序，获取的时间复杂度为O(N)
// 可能的优化点(Redis 暂时还不需要，但是...):
//  1.插入事件的时候就排序，这样最近的时间事件就是head，这样虽然会更好，但是插入和删除变成了O(N)
// 	2.使用跳表，这样获取变成了O(1)并且插入是O(log(N))
func aeSearchNearestTimer(el *EventLoop) *timeEvent {
	te := el.TimeEventHead
	var nearest *timeEvent
	for te != nil {
		if nearest == nil || te.WhenTimestamp < nearest.WhenTimestamp {
			nearest = te
		}
		te = te.Next
	}
	return nearest
}
