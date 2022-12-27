//go:build solaris

package ae

import "github.com/pengdafu/redis-golang/anet"

type aeApiState struct {
	portFd int
	Events int
}

// todo call port_create?
func aeApiCreate(el *EventLoop) error {
	state := new(aeApiState)

	el.ApiData = state

	_ = anet.Cloexec(state.portFd)
	return nil
}

func aeApiFree(el *EventLoop) {
	state := el.ApiData.(*aeApiState)
	syscall.Close(state.portFd)
}
