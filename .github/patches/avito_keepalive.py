#!/usr/bin/env python3
"""Keep the core's Avito ShareDB connections open.

Avito closes a /sharedb WebSocket that has carried nothing for a minute
(measured 2026-11-03 from a home connection: closed after 59 s with nothing
sent, open for 200 s with a ping every 20 s). The core sends nothing on an
idle room, so every room reconnected each minute: 16 rooms a minute against
Avito's ~10 handshakes a minute from one address, so the lanes were never all
up and a client could not connect. This makes each connection send a
WebSocket ping every 20 s and count the server's pong as activity for the
read deadline.

usage: avito_keepalive.py path/to/transport/avito/avito.go
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
    '\tshareDBType = "http://sharejs.org/types/textv1"\n',
    '\tshareDBType = "http://sharejs.org/types/textv1"\n'
    "\t// avitoPingInterval: Avito closes a ShareDB connection quiet for a minute.\n"
    "\tavitoPingInterval = 20 * time.Second\n",
)

patch(
    '\tw.connected.Store(true)\n\tutils.Debugf("[AVITO] WS ready: %s ver=%d docLen=%d", w.roomID[:8], w.ver, w.docLen)\n',
    '\tw.connected.Store(true)\n\tutils.Debugf("[AVITO] WS ready: %s ver=%d docLen=%d", w.roomID[:8], w.ver, w.docLen)\n'
    "\n"
    "\t// Keepalive: a ping every avitoPingInterval, and the pong counts as a\n"
    "\t// message for the read deadline, so an idle room stays connected.\n"
    "\tconn.SetPongHandler(func(string) error {\n"
    "\t\treturn conn.SetReadDeadline(time.Now().Add(w.config.WSReadTimeout))\n"
    "\t})\n"
    "\tpingStop := make(chan struct{})\n"
    "\tdefer close(pingStop)\n"
    "\tgo func() {\n"
    "\t\tt := time.NewTicker(avitoPingInterval)\n"
    "\t\tdefer t.Stop()\n"
    "\t\tfor {\n"
    "\t\t\tselect {\n"
    "\t\t\tcase <-pingStop:\n"
    "\t\t\t\treturn\n"
    "\t\t\tcase <-w.ctx.Done():\n"
    "\t\t\t\treturn\n"
    "\t\t\tcase <-t.C:\n"
    "\t\t\t\t// WriteControl may run alongside the other writes (gorilla/websocket).\n"
    "\t\t\t\tif err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(w.config.WSWriteTimeout)); err != nil {\n"
    "\t\t\t\t\treturn\n"
    "\t\t\t\t}\n"
    "\t\t\t}\n"
    "\t\t}\n"
    "\t}()\n",
)

open(path, "w", encoding="utf-8").write(src)
print("avito keepalive: patched", path)
