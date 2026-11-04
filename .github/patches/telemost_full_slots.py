#!/usr/bin/env python3
"""Ask Telemost's SFU for full-size slots, as core 0.17.1 does.

The slot size tells the SFU how large the subscriber renders each stream, and
the SFU sizes the bitrate it forwards to it. The app's core (0.15.1) asked for
608x342, so the SFU forwarded a thumbnail layer, a few hundred Kbps per lane,
to the phone however much the exit sent; core 0.17.1 measured 0.45 MB/s at
608x342 against 1.15 MB/s at 1920x1080 in the same room. The first two slots
(the ones the SFU forwards our stream to) now ask for 1920x1080.

usage: telemost_full_slots.py path/to/transport/yandex/telemost.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
old = '\t\t{"width": 608, "height": 342}, {"width": 608, "height": 342},\n'
new = ('\t\t// patched (tilvo2003-design/OpenFluxAndroid): full-size, as core 0.17.1\n'
       '\t\t{"width": 1920, "height": 1080}, {"width": 1920, "height": 1080},\n')
if src.count(old) != 1:
    if '{"width": 1920, "height": 1080}, {"width": 1920, "height": 1080},' in src:
        print("telemost full slots: already full-size upstream", path)
        sys.exit(0)
    sys.exit(f"{path}: expected exactly one 608x342 slot pair, found {src.count(old)}; the upstream code changed")
open(path, "w", encoding="utf-8").write(src.replace(old, new))
print("telemost full slots: patched", path)
