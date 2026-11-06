#!/usr/bin/env python3
"""Avito: an idle exit's lanes look for data once a second, not 20 times.

A lane with nothing to send sleeps until the stripe wakes it, and also looks
again every 50 ms in case a wakeup was missed: eight lanes, 160 wakeups a
second on an exit with nobody connected. While the exit is idle
(transport.ExitIdle) the fallback look is every second; the stripe's wakeup
still starts a send at once.

usage: avito_idle_poll.py path/to/core/transport/avito/stripe.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""			idle.Reset(lanePollIdle)
""", """			if transport.ExitIdle() {
				idle.Reset(lanePollIdleExit) // avito_idle_poll.py
			} else {
				idle.Reset(lanePollIdle)
			}
"""),
    ("""const lanePollIdle = 50 * time.Millisecond
""", """const lanePollIdle = 50 * time.Millisecond

// lanePollIdleExit is lanePollIdle on an exit nobody is connected to.
const lanePollIdleExit = time.Second
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("avito idle poll: patched", path)
