#!/usr/bin/env python3
"""New Avito rooms are joined together, over one connection.

Every new room was joined (registerRoom) over a Socket.IO connection of its
own, each one of the ~10 connections a minute Avito allows an address: new
rooms for a node took three minutes, a lane ~25 s for its new pair. One
connection joins several rooms (checked 2026-11-06), so an exit making its
rooms joins them, and the control room, over one (avito_join_batch/
join_many.go); avito_rotate's new and spare rooms go the same way.

Applies after avito_join_new.py and avito_rotate.py.

usage: avito_join_batch.py path/to/core/transport/avito
"""
import os
import shutil
import sys

pkg = sys.argv[1]
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "avito_join_batch")
path = os.path.join(pkg, "avito.go")
src = open(path, encoding="utf-8").read()
for old, new in [
    ("""		// Joined now, the room connects at its first try (patched in by
		// tilvo2003-design/OpenFluxAndroid).
		if err := registerRoom(t.ctx, roomID); err != nil {
			if t.ctx.Err() != nil {
				return nil, errStopped
			}
			utils.Debugf("[AVITO] join new room %s: %v (joined later)", roomID[:8], err)
		}
""", ""),
    ("""	t.makeCtlRoom(len(ids))
""", """	t.makeCtlRoom(len(ids))
	// All new rooms joined over one connection (avito_join_batch.py).
	if t.rot.ctlID != "" {
		t.joinRooms(append(append([]string(nil), ids...), t.rot.ctlID))
	} else {
		t.joinRooms(ids)
	}
"""),
]:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
for f in ("join_many.go", "join_many_test.go"):
    dst = os.path.join(pkg, f)
    if os.path.exists(dst):
        sys.exit(f"{dst} exists already; the upstream code changed")
    shutil.copy(os.path.join(here, f), dst)
print("avito join batch: patched", pkg)
