package ae

import "syscall"

func apiCreate(el *eventLoop) error {
	syscall.EpollCreate()
	return nil
}
