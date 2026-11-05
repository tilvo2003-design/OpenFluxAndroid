#!/usr/bin/env python3
"""An exit without a client says so to its carriers, and hands back memory.

transport.ExitIdle() is true while the exit has no client (none yet, or the
one it had is heard on no carrier for 30 s); telemost_idle.py uses it. While
idle a minute, and every 10 minutes after, the exit returns the memory it
used for a client's traffic to the system (an exit held ~200 MB for hours
after its client left, against ~30 MB before the first one came).

usage: session_exit_idle.py path/to/core/transport
"""
import os
import shutil
import sys

pkg = sys.argv[1]
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_exit_idle")
path = os.path.join(pkg, "session.go")
src = open(path, encoding="utf-8").read()
old = """func (s *Session) keepaliveLoop() {
	defer s.wg.Done()
"""
new = """func (s *Session) keepaliveLoop() {
	defer s.wg.Done()
	if s.exit { // session_exit_idle.py
		f := s.exitIdle
		exitIdleFn.Store(&f)
		go s.trimWhileIdle(30 * time.Second)
	}
"""
if src.count(old) != 1:
    sys.exit(f"{path}: keepaliveLoop changed upstream")
open(path, "w", encoding="utf-8").write(src.replace(old, new))
for f in ("exit_idle.go", "exit_idle_test.go"):
    dst = os.path.join(pkg, f)
    if os.path.exists(dst):
        sys.exit(f"{dst} exists already; the upstream code changed")
    shutil.copy(os.path.join(here, f), dst)
print("session exit idle: patched", pkg)
