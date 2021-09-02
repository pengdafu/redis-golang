package ae

import pkgTime "github.com/pengdafu/redis-golang/pkg/time"

const (
	NONE = iota << 1
	READABLE
	WRITEABLE
	BARRIER
)

const (
	FILE_EVENTS = 1 << iota
	TIME_EVENTS
	DONT_WAIT
	CALL_BEFORE_SLEEP
	CALL_AFTER_SLEEP
	ALL_EVENTS = FILE_EVENTS | TIME_EVENTS
)

const (
	OK  = 0
	ERR = -1
)

type aeBeforeSleepProc func(eventLoop *EventLoop)
type EventLoop struct {
	MaxFd           int // 多路复用io一次最多返回多少个事件的fd
	SetSize         int
	TimeEventNextId int64
	Events          []fileEvent
	Fired           []firedEvent
	TimeEventHead   *timeEvent
	Stop            int
	ApiData         interface{} // todo
	BeforeSleep     aeBeforeSleepProc
	AfterSleep      aeBeforeSleepProc
	Flags           int
}

type aeFileProc func(eventLoop *EventLoop, fd int, clientData interface{}, mask int)
type fileEvent struct {
	Mask       int // AE_(WRITEABLE|READABLE|BARRIER) 中的一种
	RFileProc  aeFileProc
	WFileProc  aeFileProc
	ClientData interface{}
}

type firedEvent struct {
	Fd   uint64
	Mask int
}

type aeTimeProc func(eventLoop *EventLoop, id uint64, clientData interface{}) int
type aeEventFinalizerProc func(eventLoop *EventLoop, clientData interface{})
type timeEvent struct {
	Id            int64
	When          int64 // timestamp
	TimeProc      aeTimeProc
	FinalizerProc aeEventFinalizerProc
	ClientData    interface{}
	Prev          *timeEvent
	Next          *timeEvent
	RefCount      int // 在递归时间事件被调用时，refCount为了防止时间器事件被释放
}

func CreateEventLoop(setsize int) (*EventLoop, error) {
	el := new(EventLoop)

	el.Events = make([]fileEvent, 0, setsize)
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
	if err := apiCreate(el); err != nil {
		return nil, err
	}

	for i := 0; i < setsize; i++ {
		el.Events[i].Mask = NONE
	}

	return el, nil
}

func (el *EventLoop) ApiPoll(tvp *pkgTime.TimeVal) int {
	return apiPoll(el, tvp)
}

func (el *EventLoop) CreateTimeEvent(milliseconds int64, proc aeTimeProc, clientData interface{}, finalizerProc aeEventFinalizerProc) int64 {
	id := el.TimeEventNextId
	el.TimeEventNextId++

	te := new(timeEvent)
	te.Id = id
	te.When = pkgTime.GetMonotonicUs() + milliseconds*1000
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
