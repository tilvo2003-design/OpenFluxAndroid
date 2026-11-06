#!/usr/bin/env python3
"""Stripe: its clocks stop when there is nothing to do.

A profile of an exit with nobody connected: a fifth of a core, two thirds of
it in Go's scheduler being woken for nothing. The stripe's reorder buffer
looked for overdue holes every 5 ms with nothing buffered (200 wakeups a
second per stripe), and its ack loop woke every ack interval (10 ms for
Telemost) to find nothing new to ack. On a phone each wakeup costs far more
than the check.

- The reorder buffer's clock runs only while it holds pieces: the first
  piece wakes it.
- The ack loop sleeps while there is nothing new and no hole; the next piece
  (or a hole given up on) wakes it, and the ack goes out on its next tick
  as before.
- The loss loop of an idle exit (transport.ExitIdle) ticks every 250 ms: no
  piece is in flight to a client that is gone.

usage: stripe_idle_wakeups.py path/to/core/transport/striped.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""	out     chan reorderOut
	gapNext bool
""", """	out     chan reorderOut
	gapNext bool
	kick    chan struct{} // a first piece buffered (stripe_idle_wakeups.py)
	changed chan struct{} // gen moved on
"""),
    ("""	r := &reorderBuffer{ring: make([]reorderSlot, reorderWindow), out: make(chan reorderOut, 8192)}
""", """	r := &reorderBuffer{ring: make([]reorderSlot, reorderWindow), out: make(chan reorderOut, 8192),
		kick: make(chan struct{}, 1), changed: make(chan struct{}, 1)}
"""),
    ("""	r.gen++ // even a duplicate or a late piece: the sender should hear we have it
""", """	r.gen++ // even a duplicate or a late piece: the sender should hear we have it
	r.notifyLocked()
"""),
    ("""	*sl = reorderSlot{seq: seq, data: data, at: now, ok: true}
	r.count++
""", """	*sl = reorderSlot{seq: seq, data: data, at: now, ok: true}
	r.count++
	if r.count == 1 {
		select {
		case r.kick <- struct{}{}:
		default:
		}
	}
"""),
    ("""		r.gapNext = true
		r.next = s
		r.gen++
	}
	r.drainLocked()
}
""", """		r.gapNext = true
		r.next = s
		r.gen++
		r.notifyLocked()
	}
	r.drainLocked()
}

// notifyLocked tells a sleeping ack loop there is something to ack.
func (r *reorderBuffer) notifyLocked() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}
"""),
    ("""func (r *reorderBuffer) run(done chan struct{}) {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
		}
		r.mu.Lock()
""", """func (r *reorderBuffer) run(done chan struct{}) {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		// Nothing buffered: nothing can be overdue until a piece comes.
		r.mu.Lock()
		empty := r.count == 0
		r.mu.Unlock()
		if empty {
			select {
			case <-done:
				return
			case <-r.kick:
			}
		}
		select {
		case <-done:
			return
		case <-t.C:
		}
		r.mu.Lock()
"""),
    ("""		case now := <-t.C:
			gen, holes, ok := s.ro.ackState()
			if !ok || (gen == lastGen && (!holes || now.Sub(lastAt) < 4*s.ackPeriod())) {
				continue
			}
""", """		case now := <-t.C:
			gen, holes, ok := s.ro.ackState()
			if !ok || (gen == lastGen && (!holes || now.Sub(lastAt) < 4*s.ackPeriod())) {
				if !ok || (gen == lastGen && !holes) {
					// Nothing to ack until a piece comes (stripe_idle_wakeups.py).
					select {
					case <-s.done:
						return
					case <-s.ro.changed:
					}
				}
				continue
			}
"""),
    ("""func (s *StripedTransport) lossLoop() {
	t := time.NewTicker(s.ackPeriod())
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-t.C:
			if s.snd.detectLoss(now) {
				s.wakePumps()
			}
			s.snd.tickLanes(now)
		}
	}
}
""", """func (s *StripedTransport) lossLoop() {
	t := time.NewTicker(s.ackPeriod())
	defer t.Stop()
	slow := false // an idle exit's loop ticks every lossIdleTick
	for {
		select {
		case <-s.done:
			return
		case now := <-t.C:
			if s.snd.detectLoss(now) {
				s.wakePumps()
			}
			s.snd.tickLanes(now)
			if idle := ExitIdle(); idle != slow {
				slow = idle
				if slow {
					t.Reset(max(lossIdleTick, s.ackPeriod()))
				} else {
					t.Reset(s.ackPeriod())
				}
			}
		}
	}
}

// lossIdleTick is the loss loop's period on an idle exit
// (stripe_idle_wakeups.py).
const lossIdleTick = 250 * time.Millisecond
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("stripe idle wakeups: patched", path)
