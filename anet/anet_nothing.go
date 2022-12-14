//go:build freebsd || solaris || windows || darwin
package anet

func anetKeepAlive(fd, interval int) error {
	return nil
}
