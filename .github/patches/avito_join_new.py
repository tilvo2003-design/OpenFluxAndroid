#!/usr/bin/env python3
"""Join each Avito room right after creating it.

A room made by the REST call is unknown to Avito's realtime side until it has
been joined once over Socket.IO. The core joined it only after its first
ShareDB connect had failed with "room is not found in MongoDB": three
handshakes per new room (the failed connect, the join, the connect again) at
Avito's one per 12 s, so a fresh node took about ten minutes to bring its 16
rooms up. Joined at creation, a room needs two, and the first start is about
a third shorter. A join that fails here is done later the old way.

usage: avito_join_new.py path/to/transport/avito/avito.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
old = """		ids = append(ids, roomID)
		utils.Debugf("[AVITO] created room %d/%d: %s", i+1, total, roomID[:8])
"""
new = """		ids = append(ids, roomID)
		utils.Debugf("[AVITO] created room %d/%d: %s", i+1, total, roomID[:8])
		// Joined now, the room connects at its first try (patched in by
		// tilvo2003-design/OpenFluxAndroid).
		if err := registerRoom(t.ctx, roomID); err != nil {
			if t.ctx.Err() != nil {
				return nil, errStopped
			}
			utils.Debugf("[AVITO] join new room %s: %v (joined later)", roomID[:8], err)
		}
"""
if src.count(old) != 1:
    sys.exit(f"{path}: expected exactly one room creation, found {src.count(old)}; the upstream code changed")
open(path, "w", encoding="utf-8").write(src.replace(old, new))
print("avito join new: patched", path)
