package transport

import (
	"runtime/debug"
	"testing"
	"time"
)

// An exit is idle before its client comes, not while it is there, and idle
// again once the client is heard on no carrier.
func TestExitIdleFollowsTheClient(t *testing.T) {
	client, exit, cw, _ := linkedSessions(t, "direct", "yandex")
	fastKeepalive(client, exit)
	if !exit.exitIdle() {
		t.Fatal("an exit without a client is not idle")
	}
	startPair(t, client, exit)
	eventually(t, "ExitIdle to see the client", func() bool { return !ExitIdle() })
	blackhole(cw["direct"], cw["yandex"])
	eventually(t, "ExitIdle once the client is gone", ExitIdle)
}

// An idle exit collects at idleGOGC, and at the configured target again once
// its client is heard.
func TestIdleExitCollectsSooner(t *testing.T) {
	old := debug.SetGCPercent(300)
	defer debug.SetGCPercent(old)
	after := idleTrimAfter.Swap(0)
	defer idleTrimAfter.Store(after)
	gogc := func() int { p := debug.SetGCPercent(-1); debug.SetGCPercent(p); return p }

	client, exit, _, _ := linkedSessions(t, "direct", "yandex")
	fastKeepalive(client, exit)
	go exit.trimWhileIdle(10 * time.Millisecond)
	eventually(t, "the idle target", func() bool { return gogc() == idleGOGC })
	startPair(t, client, exit)
	eventually(t, "the configured target with a client", func() bool { return gogc() == 300 })
}
