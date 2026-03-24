//go:build !race

package testkit

// RaceEnabled is true when the package was compiled with the go race detector
// enabled.
//
// Not sure of any better way to do it as of writing this.
const RaceEnabled = false
