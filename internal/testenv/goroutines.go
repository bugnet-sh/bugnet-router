package testenv

import (
	"runtime"
	"strings"
	"time"
)

// goroutineWait bounds how long GoroutinesGone waits for stopped goroutines to exit
const goroutineWait = 5 * time.Second

// GoroutinesGone reports whether the goroutines fn started end within 5 s, fn named as in a trace
func GoroutinesGone(fn string) bool {
	mark := "created by " + fn + " in goroutine "
	for end := time.Now().Add(goroutineWait); strings.Contains(stacks(), mark); {
		if time.Now().After(end) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// stacks is the stack trace of every goroutine
func stacks() string {
	for buf := make([]byte, 1<<16); ; buf = make([]byte, 2*len(buf)) {
		if n := runtime.Stack(buf, true); n < len(buf) {
			return string(buf[:n])
		}
	}
}
