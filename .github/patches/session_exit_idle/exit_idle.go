package transport

// An exit with nobody connected (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/session_exit_idle.py):
// carriers may idle (Telemost sends its filler slower), and the memory a
// client's traffic took goes back to the system.

import (
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

var exitIdleFn atomic.Pointer[func() bool]

// ExitIdle reports whether this process is an exit that has no client: none
// has shaken hands yet, or the one that did is heard on no carrier.
func ExitIdle() bool {
	if f := exitIdleFn.Load(); f != nil {
		return (*f)()
	}
	return false
}

func (s *Session) exitIdle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.ready || s.peerSilentLocked()
}

// trimWhileIdle hands the memory of a client's traffic back to the system
// once the exit has been idle a minute, and again every 10 minutes: Go keeps
// freed memory for reuse, and an exit kept a client's ~200 MB for hours.
func (s *Session) trimWhileIdle(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var idleSince, trimmed time.Time
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		now := time.Now()
		if !s.exitIdle() {
			idleSince, trimmed = time.Time{}, time.Time{}
			continue
		}
		if idleSince.IsZero() {
			idleSince = now
		}
		if now.Sub(idleSince) >= time.Minute && (trimmed.IsZero() || now.Sub(trimmed) >= 10*time.Minute) {
			debug.FreeOSMemory()
			trimmed = now
			utils.Debugf("[SESSION] exit idle: memory handed back to the system")
		}
	}
}
