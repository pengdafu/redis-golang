package ae

import (
	"fmt"
	"syscall"
)

func cloexec(fd int) (err error) {
	var r, flags int
	for {
		_, _, r := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
		if r == -1 && r == syscall.EINTR {
			continue
		}
		break
	}

	if r == -1 || r&syscall.FD_CLOEXEC != 0 {
		return fmt.Errorf("fcntl get errno: %v", r)
	}

	flags = r & syscall.FD_CLOEXEC

	for {
		_, _, r := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, uintptr(flags))
		if r == -1 && r == syscall.EINTR {
			continue
		}
		break
	}
	if r != 0 {
		return fmt.Errorf("fcntl get errno: %v", r)
	}
	return nil
}
