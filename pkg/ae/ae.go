package ae

const (
	NONE = iota << 1
	READABLE
	WRITEABLE
	BARRIER
)

type aeBeforeSleepProc func(eventLoop *eventLoop)
type eventLoop struct {
	MaxFd           int // 多路复用io一次最多返回多少个事件的fd
	SetSize         int
	TimeEventNextId uint64
	Events          []*fileEvent
	Fired           []*firedEvent
	TimeEventHead   *timeEvent
	Stop            int
	ApiData         interface{} // todo
	BeforeSleep     aeBeforeSleepProc
	AfterSleep      aeBeforeSleepProc
	Flags           int
}

type aeFileProc func(eventLoop *eventLoop, fd int, clientData interface{}, mask int)
type fileEvent struct {
	Mask       int // AE_(WRITEABLE|READABLE|BARRIER) 中的一种
	RFileProc  aeFileProc
	WFileProc  aeFileProc
	ClientData interface{}
}

type firedEvent struct {
	Fd   int
	Mask int
}

type aeTimeProc func(eventLoop *eventLoop, id uint64, clientData interface{})
type aeEventFinalizerProc func(eventLoop *eventLoop, clientData interface{})
type timeEvent struct {
	Id            uint64
	MonoTime      uint64 // timestamp
	TimeProc      aeTimeProc
	FinalizerProc aeEventFinalizerProc
	ClientData    interface{}
	Prev          *timeEvent
	Next          *timeEvent
	RefCount      int // 在递归时间事件被调用时，refCount为了防止时间器事件被释放
}

func CreateEventLoop(setsize int) (*eventLoop, error) {
	el := new(eventLoop)

	el.Events = make([]*fileEvent, 0, setsize)
	el.Fired = make([]*firedEvent, 0, setsize)
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

func (el *eventLoop) Main() {

}