#!/usr/bin/env python3
"""Watch lists for any striped carrier; Telemost probes another lane rarely.

The stripe sends only into lanes the far side says it hears - so far only
for Telemost, whose LaneSource reports them. SetWatchSource lets any carrier
report them (avito_stripe_tuning.py uses it), and unwatched() lets the
carrier health check leave out lanes nobody is meant to send into.

Telemost: a lane that did not pay off used to be tried again after 10
minutes, and every new lane costs all lanes 10-15 s of heavy loss (the SFU
renegotiates): a slowdown every 10 min during long transfers. Now after an
hour.

usage: stripe_watch_source.py path/to/core/transport
"""
import os
import shutil
import sys

pkg = sys.argv[1]
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stripe_watch")
path = os.path.join(pkg, "striped.go")
src = open(path, encoding="utf-8").read()
for old, new in [
    ("""	healthDeadline time.Duration
	allDeadSince   time.Time
""", """	healthDeadline time.Duration
	allDeadSince   time.Time

	// watchFrom reports the lanes this side hears when no LaneSource does
	// (stripe_watch_source.py).
	watchFrom WatchSource
"""),
    ("""	if _, ok := s.src.(WatchSource); ok {
		go s.watchLoop()
""", """	if s.watcher() != nil {
		go s.watchLoop()
"""),
    ("""	ws := s.src.(WatchSource)
""", """	ws := s.watcher()
"""),
    ("""		reprobeAge = 10 * time.Minute
""", """		reprobeAge = 60 * time.Minute // a new lane costs every lane 10-15 s (stripe_watch_source.py)
"""),
]:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:60]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
for f in ("striped_watch.go", "striped_watch_test.go"):
    dst = os.path.join(pkg, f)
    if os.path.exists(dst):
        sys.exit(f"{dst} exists already; the upstream code changed")
    shutil.copy(os.path.join(here, f), dst)
print("stripe watch source: patched", pkg)
