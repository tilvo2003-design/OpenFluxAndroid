#!/usr/bin/env python3
"""Log how many of a node's Avito rooms are connected, when it changes.

The room states are logged only with the detailed log (-dd), which also logs
every packet and restarts the node when switched on. OpenFlux Panel reads
this one line from the normal log and shows "Avito: N/M rooms" on the card.

usage: avito_rooms_up.py path/to/transport/avito/avito.go
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
    """	var lastTotalBS, lastTotalBR uint64

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-tick.C:
			var totalBS, totalBR uint64
""",
    """	var lastTotalBS, lastTotalBR uint64
	lastUp := -1

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-tick.C:
			// Rooms connected, on the normal log when it changes (patched in
			// by tilvo2003-design/OpenFluxAndroid for OpenFlux Panel).
			up := 0
			for _, ws := range all {
				if ws.connected.Load() {
					up++
				}
			}
			if up != lastUp {
				utils.Infof("[AVITO] rooms up: %d/%d", up, len(all))
				lastUp = up
			}
			var totalBS, totalBR uint64
""",
)

open(path, "w", encoding="utf-8").write(src)
print("avito rooms up: patched", path)
