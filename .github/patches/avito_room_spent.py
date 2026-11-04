#!/usr/bin/env python3
"""Leave an Avito room alone once Avito refuses any more data in it.

Avito keeps a data budget per room (measured 2026-11-04: about 20 MB of ops,
not restored after more than 12 hours). A spent room still connects, but
every op is refused with ERR_ROOM_OPERATION_LIMIT; the core dropped it,
reconnected and tried again, round after round, spending the handshake budget
and dragging the healthy rooms down with it. Now a room whose op is still
refused after the waits of avito_op_limit.py (about 16 s) stops: it logs
"[AVITO] room <id> spent" (OpenFlux Panel shows it) and stays disconnected
until the transport starts again, so the stripe carries on over the others.

Apply after avito_op_limit.py.
usage: avito_room_spent.py path/to/transport/avito/avito.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()


def patch(old, new):
    global src
    if src.count(old) != 1:
        sys.exit(f"{path}: expected exactly one {old.strip()[:60]!r}, found {src.count(old)}; the upstream code changed (or avito_op_limit.py was not applied)")
    src = src.replace(old, new)


patch(
    """	errStopped  = errors.New("avito: транспорт остановлен")
""",
    """	errStopped  = errors.New("avito: транспорт остановлен")
	// errRoomSpent: Avito refuses any more data in this room.
	errRoomSpent = errors.New("avito: room data limit reached")
""",
)

patch(
    """			return fmt.Errorf("op error: %v", errMsg)
		}
		if v, ok := reply["v"].(float64); ok {""",
    """			if isRoomSpent(errMsg) {
				return fmt.Errorf("%w: %v", errRoomSpent, errMsg)
			}
			return fmt.Errorf("op error: %v", errMsg)
		}
		if v, ok := reply["v"].(float64); ok {""",
)

patch(
    """		err := w.connectAndServe()
		w.connected.Store(false)
		if w.ctx.Err() != nil {
			return
		}
""",
    """		err := w.connectAndServe()
		w.connected.Store(false)
		if w.ctx.Err() != nil {
			return
		}
		if errors.Is(err, errRoomSpent) {
			utils.Infof("[AVITO] room %s spent: Avito takes no more data in it", w.roomID[:8])
			return
		}
""",
)

patch(
    """// isOpLimited reports whether a ShareDB error is Avito's operation limit.""",
    """// isRoomSpent reports Avito's per-room data limit (the operation limit that
// outlasted the waits).
func isRoomSpent(errMsg interface{}) bool {
	m, ok := errMsg.(map[string]interface{})
	if !ok {
		return false
	}
	code, _ := m["code"].(string)
	return code == "ERR_ROOM_OPERATION_LIMIT"
}

// isOpLimited reports whether a ShareDB error is Avito's operation limit.""",
)

open(path, "w", encoding="utf-8").write(src)
print("avito room spent: patched", path)
