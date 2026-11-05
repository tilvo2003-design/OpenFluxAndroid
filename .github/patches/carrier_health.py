#!/usr/bin/env python3
"""Fall back from a carrier that is up but delivers little.

The Session sends over the highest-priority carrier it has heard from
lately, however badly that carrier does: with cups.online above Avito, a
cups.online with most of its rooms broken kept the traffic, losing pieces
and stalling the tunnel's TCP, while Avito idled (seen 2026-11-04: lanes
DEAD, gaveUp in the hundreds). Now a striped carrier (Avito, cups.online,
Telemost) whose lanes are mostly dead - more than half of those that have
sent, each with 20 pieces unanswered for 0.6 s - is passed over while
another heard carrier is healthy: for 15 s, then it gets 5 s of traffic to
show whether it recovered, and is judged again. With every carrier degraded
nothing changes. Both peers run the same rule, so they move together.

usage: carrier_health.py path/to/core   (the module root)
"""
import os
import sys

root = sys.argv[1]
t = os.path.join(root, "transport")


def write(rel, body):
    path = os.path.join(root, rel)
    if os.path.exists(path):
        sys.exit(f"{path} exists already; the upstream code changed")
    open(path, "w", encoding="utf-8").write(body)


def patch(rel, old, new):
    path = os.path.join(root, rel)
    src = open(path, encoding="utf-8").read()
    if src.count(old) != 1:
        sys.exit(f"{path}: expected exactly one {old.strip()[:60]!r}, found {src.count(old)}; the upstream code changed")
    open(path, "w", encoding="utf-8").write(src.replace(old, new))


write("transport/carrier_health.go", '''package transport

// Carrier health for the Session's choice (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/carrier_health.py).

import (
	"sync"
	"time"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

// Degrader is a carrier that can tell it is up but delivers little.
type Degrader interface {
	Degraded() bool
}

const (
	healthCacheFor  = 250 * time.Millisecond // Degraded is asked on every packet
	carrierAvoidFor = 15 * time.Second       // a degraded carrier is passed over this long,
	carrierTrialFor = 5 * time.Second        // then carries traffic this long to show it recovered
)

type healthMemo struct {
	at  time.Time
	bad bool
}

var stripeHealth sync.Map // *StripedTransport -> healthMemo

// Degraded: more than half of the lanes that have sent are dead (each has
// 20 pieces unanswered and nothing delivered for deadAfter).
func (s *StripedTransport) Degraded() bool {
	now := time.Now()
	if m, ok := stripeHealth.Load(s); ok && now.Sub(m.(healthMemo).at) < healthCacheFor {
		return m.(healthMemo).bad
	}
	// A lane the far side says it does not hear carries nothing on purpose
	// (stripe_watch_source.py): not a dead lane.
	var skip [maxStripeLanes]bool
	for i := range skip {
		skip[i] = s.unwatched(i, now)
	}
	q := &s.snd
	q.mu.Lock()
	started, dead := 0, 0
	for i := range q.lane {
		if !q.lane[i].started || skip[i] {
			continue
		}
		started++
		if q.lane[i].dead(now) {
			dead++
		}
	}
	q.mu.Unlock()
	bad := started > 0 && dead*2 > started
	stripeHealth.Store(s, healthMemo{at: now, bad: bad})
	return bad
}

type carrierAvoid struct {
	until time.Time // passed over until then
	trial time.Time // after until: carries traffic until then whatever Degraded says
}

var avoided sync.Map // *transportLink -> carrierAvoid

func linkDegraded(l *transportLink) bool {
	d, ok := l.raw.(Degrader)
	return ok && d.Degraded()
}

// preferHealthy drops the degraded carriers from links when another one is
// healthy; links is in priority order and stays so.
func preferHealthy(links []*transportLink) []*transportLink {
	if len(links) < 2 {
		return links
	}
	now := time.Now()
	out := make([]*transportLink, 0, len(links))
	for _, l := range links {
		if v, ok := avoided.Load(l); ok {
			a := v.(carrierAvoid)
			if now.Before(a.until) {
				continue
			}
			if now.Before(a.trial) {
				out = append(out, l)
				continue
			}
			if !linkDegraded(l) {
				avoided.Delete(l)
				utils.Infof("[SESSION] carrier %q delivers again", l.name)
				out = append(out, l)
				continue
			}
			avoided.Store(l, carrierAvoid{until: now.Add(carrierAvoidFor), trial: now.Add(carrierAvoidFor + carrierTrialFor)})
			continue
		}
		if linkDegraded(l) {
			avoided.Store(l, carrierAvoid{until: now.Add(carrierAvoidFor), trial: now.Add(carrierAvoidFor + carrierTrialFor)})
			utils.Infof("[SESSION] carrier %q delivers little (most lanes dead): the next one carries the traffic for %v", l.name, carrierAvoidFor)
			continue
		}
		out = append(out, l)
	}
	if len(out) == 0 {
		return links
	}
	return out
}
''')

write("transport/carrier_health_test.go", '''package transport

import (
	"testing"
	"time"
)

type fakeDegrader struct {
	Transport
	bad bool
}

func (f *fakeDegrader) Degraded() bool { return f.bad }

func TestPreferHealthyPassesOverADegradedCarrier(t *testing.T) {
	a := &transportLink{name: "a", raw: &fakeDegrader{bad: true}}
	b := &transportLink{name: "b", raw: &fakeDegrader{}}
	defer avoided.Delete(a)
	if got := preferHealthy([]*transportLink{a, b}); len(got) != 1 || got[0] != b {
		t.Fatalf("got %v", got)
	}
	// Recovered, it waits out its pause, then carries traffic again.
	a.raw.(*fakeDegrader).bad = false
	if got := preferHealthy([]*transportLink{a, b}); len(got) != 1 || got[0] != b {
		t.Fatal("back before its pause ended")
	}
	avoided.Store(a, carrierAvoid{until: time.Now().Add(-time.Second), trial: time.Now().Add(-time.Millisecond)})
	if got := preferHealthy([]*transportLink{a, b}); len(got) != 2 || got[0] != a {
		t.Fatalf("not back after its pause: %v", got)
	}
}

func TestPreferHealthyKeepsAllWhenAllAreDegraded(t *testing.T) {
	a := &transportLink{name: "a", raw: &fakeDegrader{bad: true}}
	b := &transportLink{name: "b", raw: &fakeDegrader{bad: true}}
	defer avoided.Delete(a)
	defer avoided.Delete(b)
	if got := preferHealthy([]*transportLink{a, b}); len(got) != 2 {
		t.Fatalf("got %d links", len(got))
	}
}

func TestStripeDegradedWhenMostLanesAreDead(t *testing.T) {
	s := &StripedTransport{}
	now := time.Now()
	for i := 0; i < 4; i++ {
		s.snd.lane[i].started = true
		s.snd.lane[i].deliveredAt = now
	}
	if s.Degraded() {
		t.Fatal("healthy lanes taken for dead")
	}
	stripeHealth.Delete(s)
	for i := 0; i < 3; i++ {
		s.snd.lane[i].unanswered = 25
		s.snd.lane[i].deliveredAt = now.Add(-2 * time.Second)
	}
	if !s.Degraded() {
		t.Fatal("3 of 4 lanes dead, not degraded")
	}
}
''')

patch("transport/session.go", """	for _, name := range s.order {
		if l := s.links[name]; s.heardLocked(l) {
			out = append(out, l)
		}
	}
	if len(out) > 0 {
		return out
	}
""", """	for _, name := range s.order {
		if l := s.links[name]; s.heardLocked(l) {
			out = append(out, l)
		}
	}
	if len(out) > 0 {
		return preferHealthy(out)
	}
""")

write("transport/avito/health.go", '''package avito

// Degraded tells the Session the rooms mostly deliver nothing (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/carrier_health.py).
func (t *AvitoTransport) Degraded() bool {
	if s := t.stripe.Load(); s != nil {
		return s.Degraded()
	}
	return false
}
''')

write("transport/cupsonline/health.go", '''package cupsonline

// Degraded tells the Session the rooms mostly deliver nothing (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/carrier_health.py).
func (t *CupsonlineTransport) Degraded() bool {
	if s := t.stripe.Load(); s != nil {
		return s.Degraded()
	}
	return false
}
''')

print("carrier health: patched", root)
