#!/usr/bin/env python3
"""A client waits 60 s for the exit's handshake instead of 20 s.

Over a document carrier the first rooms take a few handshakes at Avito's
one-per-12-s pace before the exit can be heard; 20 s ran out on a connect
that would have come up.

usage: session_handshake_timeout.py path/to/transport/session.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
old = "\t\thandshakeTimeout: 20 * time.Second,\n"
if src.count(old) != 1:
    sys.exit(f"{path}: expected exactly one 20 s handshake timeout, found {src.count(old)}; the upstream code changed")
open(path, "w", encoding="utf-8").write(src.replace(old, "\t\thandshakeTimeout: 60 * time.Second, // patched: 20 s is too little over Avito\n"))
print("session handshake timeout: patched", path)
