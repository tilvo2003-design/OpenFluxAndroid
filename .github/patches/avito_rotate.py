#!/usr/bin/env python3
"""Give a lane new Avito rooms when Avito stops taking data in its rooms.

Avito takes about 20 MB into a room, then refuses every op in it for good
(ERR_ROOM_OPERATION_LIMIT; avito_room_spent.py stops writing there). New
lists get a control room (avito_rotate/rotate.go): a log in which the exit
names the rooms in use and the client reports its spent rooms. The exit
gives a spent room's lane two new rooms and both ends move that lane over;
the link stays the same. Lists without a control room work as before.

Applies after avito_keepalive, avito_op_limit, avito_room_spent,
avito_rooms_up and avito_join_new.

usage: avito_rotate.py path/to/core/transport/avito
"""
import os
import shutil
import sys

pkg = sys.argv[1]
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "avito_rotate")


def patch(name, pairs):
    path = os.path.join(pkg, name)
    src = open(path, encoding="utf-8").read()
    for old, new, count in pairs:
        if src.count(old) != count:
            sys.exit(f"{path}: expected {count} of {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
        src = src.replace(old, new)
    open(path, "w", encoding="utf-8").write(src)


for f in ("rotate.go", "rotate_test.go"):
    dst = os.path.join(pkg, f)
    if os.path.exists(dst):
        sys.exit(f"{dst} exists already; the upstream code changed")
    shutil.copy(os.path.join(here, f), dst)

patch("avito.go", [
    # the room's fields for moving to another room
    ("""	needJoin bool
""", """	needJoin bool

	// Rotation (avito_rotate.py): the room to move to, what to do when
	// Avito refuses this side's data here, roomID for other goroutines, and
	// the document's text as subscribed.
	next       atomic.Pointer[string]
	onSpent    func(id string)
	idMu       sync.Mutex
	snap       string
	budgetBase atomic.Uint64 // bytesSent+bytesRecv when this side moved into the room
	prior      atomic.Uint64 // what the room carried before this start (from the control room)
""", 1),
    ("""	data, _ := doc["data"].(string)
	w.docLen = len(data)
""", """	data, _ := doc["data"].(string)
	w.docLen = len(data)
	w.snap = data
""", 1),
    ("""	for w.ctx.Err() == nil {
		up := time.Now()
		err := w.connectAndServe()
""", """	for w.ctx.Err() == nil {
		w.takeMove()
		up := time.Now()
		err := w.connectAndServe()
""", 1),
    ("""			utils.Infof("[AVITO] room %s spent: Avito takes no more data in it", w.roomID[:8])
			return
""", """			utils.Infof("[AVITO] room %s spent: Avito takes no more data in it", w.roomID[:8])
			if !w.awaitMove() {
				return
			}
			delay = w.config.ReconnectMinDelay
			continue
""", 1),
    # a connection that came up after moveTo named a new room goes at once
    ("""
	conn.SetReadLimit(w.config.MaxMessageBytes)
""", """
	if w.next.Load() != nil {
		return errMoved
	}
	conn.SetReadLimit(w.config.MaxMessageBytes)
""", 1),
    ("""	statsStart time.Time
}
""", """	statsStart time.Time

	// rot replaces spent rooms (avito_rotate.py).
	rot rotation
}
""", 1),
    ("""	t.roomIDs, t.striped = parseRooms(rawURL)
""", """	t.roomIDs, t.striped = parseRooms(rawURL)
	if rotateOn() {
		t.rot.ctlID = parseCtl(rawURL)
	}
""", 1),
    ("""	allIDs, err := t.enterRooms()
	if err != nil {
		return err
	}
""", """	allIDs, err := t.enterRooms()
	if err != nil {
		return err
	}
	allIDs = t.startRotation(allIDs)
""", 1),
    ("""	t.rxWss = mkWS(rxIDs, t.CallReceive)
""", """	t.rxWss = mkWS(rxIDs, t.CallReceive)
	t.wireRotation()
""", 1),
    ("""	if len(ids) == 0 {
		return nil, fmt.Errorf("could not create any room")
	}
""", """	if len(ids) == 0 {
		return nil, fmt.Errorf("could not create any room")
	}
	t.makeCtlRoom(len(ids))
""", 1),
    ("transport.PackRooms(ids, t.striped)", "t.packRooms(ids)", 2),
    ("dir, i, ws.roomID[:8],", "dir, i, ws.room()[:8],", 1),
    # a variable, so the tests need not wait out the real pauses
    ("""const (
	opLimitPause = 250 * time.Millisecond
	opLimitTries = 6
)""", """var (
	opLimitPause = 250 * time.Millisecond
	opLimitTries = 6
)""", 1),
])

# The tests' fake Avito keeps each document's text and can refuse data in a room.
patch("fakeavito_test.go", [
    ("""	conns map[*fakeConn]bool
}
""", """	conns map[*fakeConn]bool
	spent map[string]bool // rooms whose ops get ERR_ROOM_OPERATION_LIMIT
}
""", 1),
    ("""	subs    map[*fakeConn]bool
}
""", """	subs    map[*fakeConn]bool
	text    string
}
""", 1),
    ("""			doc := map[string]any{"v": d.v, "data": ""}""",
     """			doc := map[string]any{"v": d.v, "data": d.text}""", 1),
    ("""			v, _ := m["v"].(float64)
			f.mu.Lock()
""", """			v, _ := m["v"].(float64)
			f.mu.Lock()
			if f.spent[room] {
				f.mu.Unlock()
				c.send(map[string]any{"a": "op", "c": "docs", "d": room, "src": m["src"], "seq": m["seq"],
					"error": map[string]any{"code": "ERR_ROOM_OPERATION_LIMIT"}})
				continue
			}
""", 1),
    ("""			d.v++
""", """			d.v++
			if ops, ok := m["op"].([]any); ok {
				if nt, ok := applyText(d.text, ops); ok {
					d.text = nt
				}
			}
""", 1),
])

print("avito rotate: patched", pkg)
