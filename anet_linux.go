package main

import (
	"syscall"
)

func anetKeepAlive(fd, interval int) error {
	val := 1
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, val); err != nil {
		return err
	}

	val = interval
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, val); err != nil {
		return err
	}

	val = interval / 3
	if val == 0 {
		val = 1
	}
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, val); err != nil {
		return err
	}

	val = 3
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, val); err != nil {
		return err
	}
	return nil
}
