//go:build windows
// +build windows

package onvif

import (
	"syscall"
)

// setSocketReuseAddr sets SO_REUSEADDR on the socket for Windows systems
func setSocketReuseAddr(fd uintptr) {
	const SO_REUSEADDR = 4
	_ = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, SO_REUSEADDR, 1)
}