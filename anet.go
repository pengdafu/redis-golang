package main

import (
	"errors"
	"fmt"
	"net"
	"syscall"
)

func anetCloexec(fd int) (err error) {
	var r, flags int
	for {
		_, _, r := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
		if r != 0 && r.Is(syscall.EINTR) {
			continue
		}
		break
	}

	if r != 0 || r&syscall.FD_CLOEXEC != 0 {
		return fmt.Errorf("fcntl get errno: %v", r)
	}

	flags = r & syscall.FD_CLOEXEC

	for {
		_, _, r := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFD, uintptr(flags))
		if r != 0 && r == syscall.EINTR {
			continue
		}
		break
	}
	if r != 0 {
		return fmt.Errorf("fcntl get errno: %v", r)
	}
	return nil
}

func anetTcpServer(port int, addr string, backlog int) (int, error) {
	return _anetTcpServer(port, addr, syscall.AF_INET, backlog)
}

func anetTcp6Server(port int, addr string, backlog int) (int, error) {
	return _anetTcpServer(port, addr, syscall.AF_INET6, backlog)
}

func _anetTcpServer(port int, addr string, af, backlog int) (s int, err error) {
	s, err = syscall.Socket(af, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		return
	}
	var socketAddr syscall.Sockaddr
	if af == syscall.AF_INET6 {
		tmp := &syscall.SockaddrInet6{
			Port: port,
		}
		copy(tmp.Addr[:], net.ParseIP(addr).To16())
		socketAddr = tmp
	} else {
		tmp := &syscall.SockaddrInet4{
			Port: port,
		}
		copy(tmp.Addr[:], net.ParseIP(addr).To4())
		socketAddr = tmp
	}

	if err := anetListen(s, socketAddr, backlog); err != nil {
		return 0, err
	}
	return
}

func anetListen(s int, addr syscall.Sockaddr, backlog int) error {
	if err := syscall.Bind(s, addr); err != nil {
		syscall.Close(s)
		return err
	}

	if err := syscall.Listen(s, backlog); err != nil {
		syscall.Close(s)
		return err
	}
	return nil
}

func anetNonBlock(fd int) error {
	return anetSetBlock(fd, true)
}

func anetSetBlock(fd int, nonBlock bool) error {
	_, _, flags := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFL, 0)
	r := int(flags)
	if flags != 0 {
		return fmt.Errorf("fcntl(F_GETFL) err: %d", flags)
	}

	if (flags&syscall.O_NONBLOCK != 0) == nonBlock {
		return nil
	}

	if nonBlock {
		r |= syscall.O_NONBLOCK
	} else {
		r &= ^syscall.O_NONBLOCK
	}

	_, _, flags = syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFL, uintptr(r))

	if flags != 0 {
		return errors.New("fcntl(F_SETFL) err")
	}

	return nil
}

func anetEnableTcpNoDelay(fd int) error {
	return anetSetTcpNoDelay(fd, 1)
}

func anetSetTcpNoDelay(fd, val int) error {
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, val); err != nil {
		return err
	}
	return nil
}

func anetAccept(s int, ip *string, port *int) (connFd int, err error) {
	return anetGenericAccept(s, ip, port)
}

func anetGenericAccept(s int, ip *string, port *int) (nfd int, err error) {
	nfd, _, err = syscall.Accept(s)
	sa, _ := syscall.Getpeername(nfd)
	if sa == nil {
		return
	}
	sd, ok := sa.(*syscall.SockaddrInet4)
	if ok {
		*port = sd.Port
		*ip = net.IP(sd.Addr[:]).String()
	}

	sd6, ok := sa.(*syscall.SockaddrInet6)
	if ok {
		*port = sd6.Port
		*ip = net.IP(sd6.Addr[:]).String()
	}
	return nfd, err
}
