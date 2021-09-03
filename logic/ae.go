package logic

import (
	"github.com/pengdafu/redis-golang/pkg/ae"
	pkgTime "github.com/pengdafu/redis-golang/pkg/time"
	"time"
)

func AeMain(el *ae.AeEventLoop) {
	el.Stop = 0
	for el.Stop == 0 {
		AeProcessEvent(el, ae.ALL_EVENTS|ae.CALL_BEFORE_SLEEP|ae.CALL_AFTER_SLEEP)
	}
}

func AeProcessEvent(el *ae.AeEventLoop, flags int) (processed int) {
	var numevents int

	// 没有时间事件和文件(IO)事件，什么也不处理
	if flags&ae.TIME_EVENTS == 0 && flags&ae.FILE_EVENTS == 0 {
		return
	}

	// 注意，只要我们处理时间事件以便于休眠到下一个时间事件触发，我们也应该要处理select() events，哪怕没有file events
	// 要处理
	if el.MaxFd != -1 || (flags&ae.TIME_EVENTS != 0 && flags&ae.DONT_WAIT == 0) {
		timeVal := &pkgTime.TimeVal{}
		msUntilTimer := int64(-1)

		if flags&ae.TIME_EVENTS != 0 && flags&ae.DONT_WAIT == 0 {
			msUntilTimer = msUntilEarliestTimer(el)
		}

		if msUntilTimer > 0 {
			timeVal.Duration = time.Millisecond * time.Duration(msUntilTimer)
		} else {
			if flags&ae.DONT_WAIT != 0 {
				//timeVal.Duration = time.Second * 0
			} else {
				timeVal = nil
			}
		}

		if el.Flags&ae.DONT_WAIT != 0 {
			timeVal = &pkgTime.TimeVal{}
		}

		if el.BeforeSleep != nil && flags&ae.CALL_BEFORE_SLEEP != 0 {
			el.BeforeSleep(el)
		}

		numevents = el.AeApiPoll(timeVal)

		if el.AfterSleep != nil && flags&ae.CALL_AFTER_SLEEP != 0 {
			el.AfterSleep(el)
		}

		for i := 0; i < numevents; i++ {
			fe := el.Events[el.Fired[i].Fd]
			fd := el.Fired[i].Fd
			mask := el.Fired[i].Mask
			fired := 0

			invert := fe.Mask & ae.BARRIER

			if invert == 0 && fe.Mask&mask&ae.READABLE != 0 {
				fe.RFileProc(int(fd), fe.ClientData, mask)
				fired++
				fe = el.Events[fd]
			}

			processed++
		}
	}

	if flags&ae.TIME_EVENTS != 0 {
		processed += processTimeEvents(el)
	}

	return processed
}

func processTimeEvents(el *ae.AeEventLoop) int {
	return 0
}

// msUntilEarliestTimer 返回距离第一个定时器被触发的时间还剩多少ms
// 如果没有定时器，返回-1
// 注意，time event 没有排序，获取的时间复杂度为O(N)
// 可能的优化点(Redis 暂时还不需要，但是...):
//  1.插入事件的时候就排序，这样最近的时间事件就是head，这样虽然会更好，但是插入和删除变成了O(N)
// 	2.使用跳表，这样获取变成了O(1)并且插入是O(log(N))
func msUntilEarliestTimer(el *ae.AeEventLoop) int64 {
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
	now := pkgTime.GetMonotonicUs()
	if now >= earliest.When {
		return 0
	}
	return (earliest.When - now) / 1000
}
