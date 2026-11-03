#!/usr/bin/env python3
"""Give back the Avito handshake turn of a room that stopped waiting.

A room takes its turn at the shared handshake budget (4 at once, then one per
12 s) when it starts waiting, and kept it when the transport stopped before
the turn came. So every connect that did not finish pushed the next one
further back: on the phone the first connect came up, the second took
longer, and from the third on the 20 s handshake ran out before a single
room dialed (seen 2026-11-03). A wait that ends with its context now hands
its turn back.

usage: avito_gate_refund.py path/to/transport/avito  (the package directory)
"""
import os
import sys

pkg = sys.argv[1]
path = os.path.join(pkg, "dialgate.go")
src = open(path, encoding="utf-8").read()
old = """		g.next = at.Add(g.dialSpacing())
		g.mu.Unlock()
		if !sleepCtx(ctx, d) {
			return false
		}
"""
new = """		g.next = at.Add(g.dialSpacing())
		g.mu.Unlock()
		if !sleepCtx(ctx, d) {
			if reserved {
				g.refund()
			}
			return false
		}
"""
if src.count(old) != 1:
    sys.exit(f"{path}: expected exactly one wait sleep, found {src.count(old)}; the upstream code changed")
src = src.replace(old, new)
src += """
// refund gives back the turn of a dial that will not happen (its wait ended
// with the context), so a stopped transport does not hold the next one back.
// Patched in by tilvo2003-design/OpenFluxAndroid.
func (g *dialGate) refund() {
	refill, _ := g.budget()
	if refill <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.adopt()
	now := time.Now()
	g.tat = g.tat.Add(-refill)
	if g.tat.Before(now) {
		g.tat = now
	}
	g.persist()
}
"""
open(path, "w", encoding="utf-8").write(src)

test = """package avito

import (
	"context"
	"testing"
	"time"
)

// A room that stops waiting gives its turn back (patched in by
// tilvo2003-design/OpenFluxAndroid).
func TestDialGateRefundsAbandonedTurns(t *testing.T) {
	g := &dialGate{refill: time.Second, burst: 1, spacing: time.Millisecond}
	if !g.wait(context.Background()) {
		t.Fatal("first dial refused")
	}
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		if g.wait(ctx) {
			t.Fatal("a dial waiting for its turn went through")
		}
		cancel()
	}
	g.mu.Lock()
	ahead := time.Until(g.tat)
	g.mu.Unlock()
	if ahead > 1100*time.Millisecond {
		t.Fatalf("budget %v ahead after five abandoned waits, want about one refill", ahead)
	}
}
"""
open(os.path.join(pkg, "dialgate_refund_test.go"), "w", encoding="utf-8").write(test)
print("avito gate refund: patched", path)
