#!/usr/bin/env python3
"""Tell wlynxg/anet the core runs on Android 11 or later.

pion lists the network interfaces through anet, which on Android 11+ avoids
the netlink calls Android forbids apps. anet learns the Android version only
from cgo (android_get_device_api_level); a CGO_ENABLED=0 android build (the
node for Termux) gets -1, takes the plain Go path, and WebRTC fails with
"route ip+net: netlinkrib: permission denied" (seen on a Galaxy A54,
2026-11-04). The anet path works on older Android too.

usage: android_anet.py path/to/core   (the module root)
"""
import os
import sys

root = sys.argv[1]
path = os.path.join(root, "android_anet.go")
if os.path.exists(path):
    sys.exit(f"{path} exists already; the upstream code changed")
open(path, "w", encoding="utf-8").write('''//go:build android

package main

import "github.com/wlynxg/anet"

// A CGO_ENABLED=0 android build cannot ask the system for its API level, and
// anet would then use the netlink calls Android 11+ forbids. Patched in by
// tilvo2003-design/OpenFluxAndroid (.github/patches/android_anet.py).
func init() { anet.SetAndroidVersion(11) }
''')
print("android anet: wrote", path)
