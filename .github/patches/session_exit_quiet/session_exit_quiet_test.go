package transport

import (
	"testing"
	"time"
)

func sentAll(ws map[string]*startCountingWire) int {
	n := 0
	for _, w := range ws {
		n += sentCount(w)
	}
	return n
}

// An exit whose client has gone sends nothing more on any carrier; once the
// client is heard again, the exit's keepalives resume.
func TestExitFallsQuietWhenClientIsGone(t *testing.T) {
	client, exit, cw, ew := linkedSessions(t, "direct", "yandex")
	fastKeepalive(client, exit)
	startPair(t, client, exit)
	eventually(t, "the exit to ping", func() bool {
		before := sentAll(ew)
		time.Sleep(60 * time.Millisecond)
		return sentAll(ew) > before
	})

	blackhole(cw["direct"], cw["yandex"]) // the client is gone: nothing of it arrives
	time.Sleep(exit.linkTimeout + 3*exit.keepaliveInterval)
	before := sentAll(ew)
	time.Sleep(10 * exit.keepaliveInterval)
	if after := sentAll(ew); after != before {
		t.Fatalf("the exit sent %d packets with its client gone", after-before)
	}

	for _, w := range []*startCountingWire{cw["direct"], cw["yandex"]} {
		w.mu.Lock()
		w.drop = false
		w.mu.Unlock()
	}
	eventually(t, "the exit's keepalives to resume", func() bool {
		before := sentAll(ew)
		time.Sleep(5 * exit.keepaliveInterval)
		return sentAll(ew) > before
	})
}
