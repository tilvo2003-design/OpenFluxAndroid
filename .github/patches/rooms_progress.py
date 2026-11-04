#!/usr/bin/env python3
"""Log each room an exit creates on the normal log (Avito, cups.online).

OpenFlux Panel shows "rooms created: N of M" from these lines; they were on
the detailed log only, so room creation was a bare timer.

usage: rooms_progress.py path/to/core   (the module root)
"""
import os
import sys

root = sys.argv[1]
for rel, old in (
    ("transport/avito/avito.go", 'utils.Debugf("[AVITO] created room %d/%d: %s", i+1, total, roomID[:8])'),
    ("transport/cupsonline/cupsonline.go", 'utils.Debugf("[CUPS] created room %d/%d: %s", i+1, n, a.roomUUID)'),
):
    path = os.path.join(root, rel)
    src = open(path, encoding="utf-8").read()
    if src.count(old) != 1:
        sys.exit(f"{path}: expected exactly one {old!r}, found {src.count(old)}; the upstream code changed")
    open(path, "w", encoding="utf-8").write(src.replace(old, old.replace("utils.Debugf(", "utils.Infof(", 1)))
print("rooms progress: patched", root)
