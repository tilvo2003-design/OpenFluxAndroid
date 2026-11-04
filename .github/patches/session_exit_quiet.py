#!/usr/bin/env python3
"""An exit stops its keepalives once its client is gone.

After a client's first handshake the exit's Session stays ready for good and
pings every carrier it has not heard from for 10 s - with the client gone,
forever: a ping on each carrier every 10 s, resent on every lane of the
striped ones (Avito, cups.online, Telemost) until given up on. That is the
traffic an exit without a client kept showing (~0.8 kbit/s), and its CPU.
Now an exit that has heard its client on no carrier for linkTimeout (30 s)
sends no keepalives until the client is heard again; a client that is there
pings every carrier itself every 10 s, and the exit answers each ping.

usage: session_exit_quiet.py path/to/core/transport
"""
import os
import shutil
import sys

pkg = sys.argv[1]
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_exit_quiet")

path = os.path.join(pkg, "session.go")
src = open(path, encoding="utf-8").read()
pairs = [
    ("""	tick := time.NewTicker(s.keepaliveInterval)
	defer tick.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-tick.C:
		}
		s.mu.Lock()
""", """	tick := time.NewTicker(s.keepaliveInterval)
	defer tick.Stop()
	wasGone := false
	for {
		select {
		case <-s.done:
			return
		case <-tick.C:
		}
		s.mu.Lock()
		// An exit whose client is heard on no carrier pings nobody (patched in
		// by tilvo2003-design/OpenFluxAndroid, session_exit_quiet.py).
		gone := s.exit && s.ready && s.peerSilentLocked()
		if gone != wasGone {
			wasGone = gone
			if gone {
				utils.Infof("[SESSION] client silent on every carrier for %v: no keepalives until it is heard again", s.linkTimeout)
			} else {
				utils.Debugf("[SESSION] client heard again: keepalives resume")
			}
		}
""", 1),
    ("""		var quiet []*transportLink
		if s.ready {
""", """		var quiet []*transportLink
		if s.ready && !gone {
""", 1),
]
for old, new, count in pairs:
    if src.count(old) != count:
        sys.exit(f"{path}: expected {count} of {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)

dst = os.path.join(pkg, "session_exit_quiet_test.go")
if os.path.exists(dst):
    sys.exit(f"{dst} exists already; the upstream code changed")
shutil.copy(os.path.join(here, "session_exit_quiet_test.go"), dst)
print("session exit quiet: patched", pkg)
