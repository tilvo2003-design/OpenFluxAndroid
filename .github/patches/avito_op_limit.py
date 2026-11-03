#!/usr/bin/env python3
"""Wait out Avito's per-room operation limit instead of dropping the room.

Under load Avito refuses an op with ERR_ROOM_OPERATION_LIMIT ("Room operation
limit exceeded") or ERR_ACTION_LIMIT_UNAVAILABLE ("ShareDB action limiter
unavailable") (seen 2026-11-03 at ~20 Mbit/s). The core closed the room on
any op error, and the reconnects spent the handshake budget until Avito
refused handshakes too. Now a limited op waits (0.25 s, doubling, six times
at most) and goes again on the same connection.

usage: avito_op_limit.py path/to/transport/avito/avito.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()


def patch(old, new):
    global src
    if src.count(old) != 1:
        sys.exit(f"{path}: expected exactly one {old.strip()[:60]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)


patch(
    """	if err := w.writeJSON(map[string]interface{}{
		"a": "op", "c": "docs", "d": w.roomID,
		"v": w.ver, "src": w.connID, "seq": w.seq, "op": op,
	}); err != nil {
		return err
	}

	// Wait for the ack from readLoop.
	select {
	case reply := <-c.ack:
		if errMsg := reply["error"]; errMsg != nil {
			w.stats.errors.Add(1)
			return fmt.Errorf("op error: %v", errMsg)
		}
		if v, ok := reply["v"].(float64); ok {
			w.ver = int(v) + 1
		} else {
			w.ver++
		}
	case <-c.done:
		return errStopped
	case <-w.ctx.Done():
		return errStopped
	}
""",
    """	for try := 0; ; try++ {
		if err := w.writeJSON(map[string]interface{}{
			"a": "op", "c": "docs", "d": w.roomID,
			"v": w.ver, "src": w.connID, "seq": w.seq, "op": op,
		}); err != nil {
			return err
		}

		// Wait for the ack from readLoop.
		var reply map[string]interface{}
		select {
		case reply = <-c.ack:
		case <-c.done:
			return errStopped
		case <-w.ctx.Done():
			return errStopped
		}
		if errMsg := reply["error"]; errMsg != nil {
			w.stats.errors.Add(1)
			// Avito's operation limit: wait and send the op again on this
			// connection (patched in by tilvo2003-design/OpenFluxAndroid).
			if isOpLimited(errMsg) && try < opLimitTries {
				pause := opLimitPause << try
				utils.Debugf("[AVITO] op limited (%s): %v; again in %v", w.roomID[:8], errMsg, pause)
				w.seq++
				t := time.NewTimer(pause)
				select {
				case <-t.C:
				case <-c.done:
					t.Stop()
					return errStopped
				case <-w.ctx.Done():
					t.Stop()
					return errStopped
				}
				continue
			}
			return fmt.Errorf("op error: %v", errMsg)
		}
		if v, ok := reply["v"].(float64); ok {
			w.ver = int(v) + 1
		} else {
			w.ver++
		}
		break
	}
""",
)

patch(
    """func (w *avitoWS) Send(data []byte) error {""",
    """// Avito's per-room operation limit: the op is refused, the connection stays.
const (
	opLimitPause = 250 * time.Millisecond
	opLimitTries = 6
)

// isOpLimited reports whether a ShareDB error is Avito's operation limit.
func isOpLimited(errMsg interface{}) bool {
	m, ok := errMsg.(map[string]interface{})
	if !ok {
		return false
	}
	code, _ := m["code"].(string)
	return code == "ERR_ROOM_OPERATION_LIMIT" || code == "ERR_ACTION_LIMIT_UNAVAILABLE"
}

func (w *avitoWS) Send(data []byte) error {""",
)

open(path, "w", encoding="utf-8").write(src)
print("avito op limit: patched", path)
