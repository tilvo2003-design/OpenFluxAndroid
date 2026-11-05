#!/usr/bin/env python3
"""Avito's stripe: the settings of a slow relay, and only into rooms the far side reads.

The Avito stripe ran on the defaults made for Telemost (CoDel dropping what
waited 150 ms, holes given up after 5 s) though a room answers an op in
0.4-1.5 s and resends after 3-10 s: the queue a round trip always builds was
dropped as "standing", and pieces were given up on before their resend came
([STRIPE] gave up ... after 5.004s on the node). It now gets what cups.online,
a relay of the same kind, has: CoDel at 1.5 s / 6 s, holes waited for 12 s
(longer than the 10 s resend ceiling), acks every 100 ms, no SFU pacing.

A lane counted as up once this side's send room was, read on the far side or
not; a phone brings its rooms up one every 12 s, and for minutes the exit's
pieces went into rooms nobody read. Each side now tells the other which rooms
it reads (stripe_watch_source.py): the lane's id is its send room, the far
side lists the rooms it is subscribed to. The list rides in the ops a room
sends anyway (data, acks), so an idle tunnel sends no extra ops.

Applies after avito_rotate.py and stripe_watch_source.py.

usage: avito_stripe_tuning.py path/to/core/transport/avito
"""
import os
import shutil
import sys

pkg = sys.argv[1]


def patch(name, pairs):
    path = os.path.join(pkg, name)
    src = open(path, encoding="utf-8").read()
    for old, new, count in pairs:
        if src.count(old) != count:
            sys.exit(f"{path}: expected {count} of {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
        src = src.replace(old, new)
    open(path, "w", encoding="utf-8").write(src)


patch("stripe.go", [
    ("""	InitRTO: 3 * time.Second,
	MinRTO:  2 * time.Second,
	MaxRTO:  10 * time.Second,
}
""", """	InitRTO: 3 * time.Second,
	MinRTO:  2 * time.Second,
	MaxRTO:  10 * time.Second,
	// A relay that answers in 0.4-1.5 s, like cups.online (avito_stripe_tuning.py):
	// CoDel above the round trip, holes waited for longer than MaxRTO, few acks
	// (each rides in an op), no pacing (made for an SFU that drops).
	CodelTarget:   1500 * time.Millisecond,
	CodelInterval: 6 * time.Second,
	HoleDeadline:  12 * time.Second,
	AckInterval:   100 * time.Millisecond,
	NoPacing:      true,
}
""", 1),
    ("""	st.SetOptions(stripeOptions)
""", """	st.SetOptions(stripeOptions)
	st.SetWatchSource(t)
""", 1),
    ("""		if len(blob) == 0 {
			idle.Reset(lanePollIdle)
""", """		// The watch list goes along with what the room sends anyway.
		if len(blob) > 0 {
			if f := w.ctlFrame.Swap(nil); f != nil {
				binary.BigEndian.PutUint32(hdr[:], uint32(len(*f)))
				blob = append(blob, hdr[:]...)
				blob = append(blob, *f...)
			}
		}
		if len(blob) == 0 {
			idle.Reset(lanePollIdle)
""", 1),
    ("""// Send carries the stripe's out-of-band frames; its data comes by pull.
func (l *avitoLane) Send(b []byte) error { return l.tx.Send(b) }
""", """// Send carries the stripe's out-of-band frames (the watch list, every
// second): the newest waits for the next op the room sends, and never blocks.
// Its data comes by pull.
func (l *avitoLane) Send(b []byte) error {
	f := append([]byte(nil), b...)
	l.tx.ctlFrame.Store(&f)
	return nil
}

// LaneID is the room this lane sends into: the far side lists it as watched
// once it reads that room.
func (l *avitoLane) LaneID() string { return l.tx.room() }
""", 1),
])

patch("avito.go", [
    ("""	wake   chan struct{}
}
""", """	wake   chan struct{}

	// ctlFrame is the stripe's newest out-of-band frame, sent with the next op
	// (avito_stripe_tuning.py).
	ctlFrame atomic.Pointer[[]byte]
}
""", 1),
])

path = os.path.join(pkg, "watch.go")
if os.path.exists(path):
    sys.exit(f"{path} exists already; the upstream code changed")
open(path, "w", encoding="utf-8").write('''package avito

// Watching lists the rooms this side reads, for the far side's stripe
// (patched in by tilvo2003-design/OpenFluxAndroid, avito_stripe_tuning.py).
func (t *AvitoTransport) Watching() []string {
	var out []string
	for _, w := range t.rxWss {
		if w.connected.Load() {
			out = append(out, w.room())
		}
	}
	return out
}
''')
dst = os.path.join(pkg, "watch_test.go")
if os.path.exists(dst):
    sys.exit(f"{dst} exists already; the upstream code changed")
shutil.copy(os.path.join(os.path.dirname(os.path.abspath(__file__)), "avito_stripe_tuning", "watch_test.go"), dst)
print("avito stripe tuning: patched", pkg)
