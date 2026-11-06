#!/usr/bin/env python3
"""Avito: a quiet lane looks for data once a second, not 20 times.

A lane with nothing to send sleeps until the stripe wakes it, and also looks
again every 50 ms in case a wakeup was missed: eight lanes, 160 wakeups a
second on an exit with nobody connected, and on a phone whose session runs
over direct. A lane that has sent nothing for 2 s, or whose exit is idle
(transport.ExitIdle), looks every second; the stripe's wakeup still starts
a send at once.

usage: avito_idle_poll.py path/to/core/transport/avito/stripe.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""			idle.Reset(lanePollIdle)
""", """			if time.Since(sentAt) > 2*time.Second || transport.ExitIdle() {
				idle.Reset(lanePollIdleExit) // avito_idle_poll.py
			} else {
				idle.Reset(lanePollIdle)
			}
"""),
    ("""		if err := w.sendOp(c, blob); err != nil {
			return err
		}
	}
}
""", """		if err := w.sendOp(c, blob); err != nil {
			return err
		}
		sentAt = time.Now()
	}
}
"""),
    ("""	idle := time.NewTimer(time.Hour)
	defer idle.Stop()
	var blob []byte
""", """	idle := time.NewTimer(time.Hour)
	defer idle.Stop()
	var blob []byte
	var sentAt time.Time // the lane's last op (avito_idle_poll.py)
"""),
    ("""const lanePollIdle = 50 * time.Millisecond
""", """const lanePollIdle = 50 * time.Millisecond

// lanePollIdleExit is lanePollIdle on a quiet lane.
const lanePollIdleExit = time.Second
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("avito idle poll: patched", path)
