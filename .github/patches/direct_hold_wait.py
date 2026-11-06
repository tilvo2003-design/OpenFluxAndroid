#!/usr/bin/env python3
"""direct: records waiting for a connection wait for it, not on a 20 ms clock.

The session pings every carrier now and then, direct included. On an exit
whose client is on another carrier, direct has no connection, so its writer
held the ping and looked for a connection every 20 ms - 50 wakeups a second
for as long as the client stayed away. Now a new connection wakes the
writer (and it still looks once a second).

usage: direct_hold_wait.py path/to/core/transport/direct.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""		space:         make(chan struct{}, 1),
""", """		space:         make(chan struct{}, 1),
		connUp:        make(chan struct{}, 1),
"""),
    ("""	space       chan struct{} // the writer signals freed budget
""", """	space       chan struct{} // the writer signals freed budget
	connUp      chan struct{} // a connection came up (direct_hold_wait.py)
"""),
    ("""	t.mu.Lock()
	t.conn = conn
	t.lastRecord.Store(time.Now().UnixNano())
	t.mu.Unlock()
""", """	t.mu.Lock()
	t.conn = conn
	t.lastRecord.Store(time.Now().UnixNano())
	t.mu.Unlock()
	select {
	case t.connUp <- struct{}{}:
	default:
	}
"""),
    ("""			select {
			case <-t.done:
				utils.Debugf("[DIRECT] writerLoop: done during wait, exiting")
				return
			case <-time.After(20 * time.Millisecond):
			}
			continue
""", """			select {
			case <-t.done:
				utils.Debugf("[DIRECT] writerLoop: done during wait, exiting")
				return
			case <-t.connUp:
			case <-time.After(time.Second):
			}
			continue
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("direct hold wait: patched", path)
