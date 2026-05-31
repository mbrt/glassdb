//go:build !linux

package main

import "time"

// processCPUTime is a no-op fallback on non-Linux platforms. The benchmark runs
// on Linux (the deploy target), where getrusage provides real CPU accounting.
func processCPUTime() (user, sys time.Duration) {
	return 0, 0
}
