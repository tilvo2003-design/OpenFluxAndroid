#!/usr/bin/env python3
"""An exit's UDP flow ends 2 min after the client last sent in it.

Every packet either way kept a UDP flow alive, so a server that keeps
sending (a call, a game, a peer that does not know the phone is gone) held
it open for good after the client left: the exit pushed what came in to the
gone client, a couple of packets a second for hours - the small traffic of
an exit with nobody connected, and Avito room budget spent on it. Now only
the client's packets keep the flow; what comes in only keeps it while the
client takes part (QUIC, calls and DNS all send both ways).

usage: exit_udp_idle.py path/to/core/tunnel/tunnel.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""		pump := func(dst, src net.Conn) {
			defer once.Do(closeBoth)
			buf := make([]byte, 65535)
			for {
				n, err := src.Read(buf)
				if err != nil {
					return
				}
				refresh()
				_ = dst.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := dst.Write(buf[:n]); err != nil {
					return
				}
			}
		}
		done := make(chan struct{})
		go func() { defer close(done); pump(remote, local) }()
		pump(local, remote)
""", """		// Only the client's packets keep the flow (patched in by
		// tilvo2003-design/OpenFluxAndroid, exit_udp_idle.py).
		pump := func(dst, src net.Conn, keeps bool) {
			defer once.Do(closeBoth)
			buf := make([]byte, 65535)
			for {
				n, err := src.Read(buf)
				if err != nil {
					return
				}
				if keeps {
					refresh()
				}
				_ = dst.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := dst.Write(buf[:n]); err != nil {
					return
				}
			}
		}
		done := make(chan struct{})
		go func() { defer close(done); pump(remote, local, true) }()
		pump(local, remote, false)
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("exit udp idle: patched", path)
