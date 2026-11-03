#!/usr/bin/env python3
"""Bring a side's Avito rooms up in lanes (send room i with receive room i).

Avito lets in about ten handshakes a minute from one address, so the core
connects its rooms one by one, 4 at once and then one per 12 s. It started
all the send rooms first and the receive rooms after them, so for the first
minute a side could only send: a client's hellos reached the exit, the exit's
answers went to rooms the client was not reading yet, and the app gave up
with a handshake timeout after 30 s (seen 2026-11-03: the exit logged the
client's hellos #1..#65 with no echo). Starting the rooms pair by pair makes
the first four handshakes two whole lanes.

usage: avito_pairs.py path/to/transport/avito/avito.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
old = """	for _, ws := range t.txWss {
		go ws.run()
	}
	for _, ws := range t.rxWss {
		go ws.run()
	}
"""
new = """	// Lanes first: send room i and receive room i together. The rooms take
	// turns at Avito's handshake budget in the order they start, and a side
	// with only its send rooms up can neither hear its peer nor finish the
	// handshake (patched in by tilvo2003-design/OpenFluxAndroid).
	for i := 0; i < max(len(t.txWss), len(t.rxWss)); i++ {
		if i < len(t.txWss) {
			go t.txWss[i].run()
			time.Sleep(5 * time.Millisecond)
		}
		if i < len(t.rxWss) {
			go t.rxWss[i].run()
			time.Sleep(5 * time.Millisecond)
		}
	}
"""
if src.count(old) != 1:
    sys.exit(f"{path}: expected exactly one room start loop, found {src.count(old)}; the upstream code changed")
open(path, "w", encoding="utf-8").write(src.replace(old, new))
print("avito pairs: patched", path)
