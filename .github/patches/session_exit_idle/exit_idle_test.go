package transport

import "testing"

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
