#!/usr/bin/env python3
"""Avito rooms share ShareDB connections, four to one.

Every room had its own connection, and Avito takes about 10 new connections
a minute from an address: a phone brought its 16 rooms up over ~2.5 minutes,
and each room that moved waited for a connection of its own. One connection
can carry several rooms (checked 2026-11-05 from the node: 4 rooms on one
connection, every op acked and relayed, 4.7 MB/s against 1.0 for one room),
so the rooms now share them (avito_mux/mux.go): a side needs 4 connections
for 16 rooms plus the control room's, and a room that moves only subscribes.
AVITO_MUX_ROOMS sets the rooms per connection (1: one each, as before).

Applies after avito_rotate.py.

usage: avito_mux.py path/to/core/transport/avito
"""
import os
import shutil
import sys

pkg = sys.argv[1]
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "avito_mux")

path = os.path.join(pkg, "avito.go")
src = open(path, encoding="utf-8").read()
for old, new, count in [
    ("""	conn    *websocket.Conn
""", """	conn    wsConn
	// mux is the shared connection the room is on, nil for a connection of
	// its own (avito_mux.py).
	mux *muxConn
""", 1),
    ("""func (w *avitoWS) dial() (*websocket.Conn, error) {
""", """func (w *avitoWS) dial() (wsConn, error) {
	if w.mux != nil {
		return w.mux.attach(w)
	}
""", 1),
    ("""func (w *avitoWS) readLoop(conn *websocket.Conn, c *avitoConn) error {""",
     """func (w *avitoWS) readLoop(conn wsConn, c *avitoConn) error {""", 1),
    ("""	w.seq = 1
""", """	w.seq = w.seqStart()
""", 1),
    ("""		w.seq = 2
""", """		w.seq++
""", 1),
    ("""	t.wireRotation()
""", """	t.wireRotation()
	t.wireMux()
""", 1),
]:
    if src.count(old) != count:
        sys.exit(f"{path}: expected {count} of {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)

# The fake Avito of the tests answers a connection's ops one after another
# (Avito does not: 4.7 MB/s on one connection against 1.0 for one room), so
# the test that measures how lanes add up gives each room its own.
path = os.path.join(pkg, "stripe_test.go")
src = open(path, encoding="utf-8").read()
old = """func TestStripedPairsAddUp(t *testing.T) {
	if raceEnabled {
		t.Skip("a throughput measurement: meaningless under the race detector")
	}
"""
if src.count(old) != 1:
    sys.exit(f"{path}: TestStripedPairsAddUp changed upstream")
src = src.replace(old, old + """	t.Setenv("AVITO_MUX_ROOMS", "1") // the fake serves a connection's ops one by one (avito_mux.py)
""")
open(path, "w", encoding="utf-8").write(src)

for f in ("mux.go", "mux_test.go"):
    dst = os.path.join(pkg, f)
    if os.path.exists(dst):
        sys.exit(f"{dst} exists already; the upstream code changed")
    shutil.copy(os.path.join(here, f), dst)
print("avito mux: patched", pkg)
