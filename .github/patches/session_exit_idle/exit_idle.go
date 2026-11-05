package transport

// An exit with nobody connected (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/session_exit_idle.py):
// carriers may idle (Telemost sends its filler slower), and the memory a
// client's traffic took goes back to the system.

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
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

// idleGOGC is the GC target of an idle exit (OPENFLUX_IDLE_GOGC, 0 keeps
// the configured one). The panel runs the nodes at GOGC=300 for speed: the
// heap may grow to four times what is live before a collection, and an idle
// exit's live ~40 MB then took ~180 MB, given back by FreeOSMemory and taken
// again within seconds by whatever still arrives.
var idleGOGC = func() int {
	if n, err := strconv.Atoi(os.Getenv("OPENFLUX_IDLE_GOGC")); err == nil && n >= 0 {
		return n
	}
	return 50
}()

// idleTrimAfter is how long an exit is idle before it trims (atomic: tests
// shorten it while other sessions' loops run).
var idleTrimAfter atomic.Int64

func init() { idleTrimAfter.Store(int64(time.Minute)) }

// trimWhileIdle hands the memory of a client's traffic back to the system
// once the exit has been idle a minute, and again every 10 minutes: Go keeps
// freed memory for reuse, and an exit kept a client's ~200 MB for hours.
// While idle the GC target is idleGOGC; the configured one is back the
// moment the client is heard.
func (s *Session) trimWhileIdle(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var idleSince, trimmed time.Time
	gogc := -1 // the configured target while ours is in force
	defer func() {
		if gogc >= 0 {
			debug.SetGCPercent(gogc)
		}
	}()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		now := time.Now()
		if !s.exitIdle() {
			if gogc >= 0 {
				debug.SetGCPercent(gogc)
				gogc = -1
			}
			idleSince, trimmed = time.Time{}, time.Time{}
			continue
		}
		if idleSince.IsZero() {
			idleSince = now
		}
		if now.Sub(idleSince) >= time.Duration(idleTrimAfter.Load()) && (trimmed.IsZero() || now.Sub(trimmed) >= 10*time.Minute) {
			if gogc < 0 && idleGOGC > 0 {
				if prev := debug.SetGCPercent(idleGOGC); prev > idleGOGC {
					gogc = prev
				} else {
					debug.SetGCPercent(prev) // already lower (or off): leave it
				}
			}
			debug.FreeOSMemory()
			trimmed = now
			utils.Debugf("[SESSION] exit idle: memory handed back to the system")
		}
	}
}

// idleProcs is how many threads an idle exit runs Go code on
// (OPENFLUX_IDLE_PROCS, 0 keeps them all). An idle exit's work is a few
// small packets and timers a second, but each wakeup on an 8-core phone set
// the scheduler hunting across eight Ps for more: a fresh exit with nobody
// connected spent 70% of its ~30% of a core in the scheduler, not in work.
var idleProcs = func() int {
	if n, err := strconv.Atoi(os.Getenv("OPENFLUX_IDLE_PROCS")); err == nil && n >= 0 {
		return n
	}
	return 2
}()

// idleProcsAfter is how long an exit is idle before it narrows (atomic, as
// idleTrimAfter).
var idleProcsAfter atomic.Int64

func init() { idleProcsAfter.Store(int64(30 * time.Second)) }

// procsWhileIdle narrows the exit to idleProcs once it has been without a
// client idleProcsAfter, and gives back every thread within a second of the
// client being heard.
func (s *Session) procsWhileIdle(every time.Duration) {
	if idleProcs <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	full := 0 // the thread count to go back to while ours is in force
	var idleSince time.Time
	defer func() {
		if full > 0 {
			runtime.GOMAXPROCS(full)
		}
	}()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		idle := s.exitIdle()
		if !idle {
			idleSince = time.Time{}
		} else if idleSince.IsZero() {
			idleSince = time.Now()
		}
		switch {
		case idle && full == 0 && time.Since(idleSince) >= time.Duration(idleProcsAfter.Load()):
			if n := runtime.GOMAXPROCS(0); n > idleProcs {
				full = runtime.GOMAXPROCS(idleProcs)
				utils.Debugf("[SESSION] exit idle: Go on %d of %d threads", idleProcs, full)
			}
		case !idle && full > 0:
			runtime.GOMAXPROCS(full)
			full = 0
		}
	}
}
