//go:build linux

package main

import (
	"syscall"
	"time"
)

// processCPUTime returns the cumulative user and system CPU time consumed by
// the whole process so far, via getrusage(RUSAGE_SELF). Comparing the delta
// over a step against the wall-clock time and the core count tells us whether
// the throughput plateau is a client CPU bottleneck.
func processCPUTime() (user, sys time.Duration) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	return time.Duration(ru.Utime.Nano()), time.Duration(ru.Stime.Nano())
}
