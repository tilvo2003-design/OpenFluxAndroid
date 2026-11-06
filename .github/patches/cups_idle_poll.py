#!/usr/bin/env python3
"""cups.online: a quiet lane looks for data once a second, not 20 times.

As avito_idle_poll.py: a lane with nothing to send also looked again every
50 ms in case a wakeup was missed, and a lane whose room is not connected
looked every 50 ms for it to be. A lane that has sent nothing for 2 s (or
whose exit is idle) looks every second, a disconnected one every 250 ms;
the stripe's wakeup still starts a send at once.

usage: cups_idle_poll.py path/to/core/transport/cupsonline/cupsonline.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""		if !w.connected.Load() {
			if !sleepCtx(w.ctx, 50*time.Millisecond) {
				return
			}
			continue
		}
		// Ack gate:""", """		if !w.connected.Load() {
			if !sleepCtx(w.ctx, 250*time.Millisecond) { // cups_idle_poll.py
				return
			}
			continue
		}
		// Ack gate:"""),
    ("""				next = nil
				continue
			}
			idle.Reset(lanePollIdle)
""", """				next = nil
				continue
			}
			if time.Since(w.lastSend) > 2*time.Second || transport.ExitIdle() {
				idle.Reset(lanePollQuiet) // cups_idle_poll.py
			} else {
				idle.Reset(lanePollIdle)
			}
"""),
    ("""const lanePollIdle = 50 * time.Millisecond
""", """const lanePollIdle = 50 * time.Millisecond

// lanePollQuiet is lanePollIdle on a quiet lane (cups_idle_poll.py).
const lanePollQuiet = time.Second
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("cups idle poll: patched", path)
