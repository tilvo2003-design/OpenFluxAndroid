#!/usr/bin/env python3
"""cups.online: both ends enter the rooms as the session that created them.

Since 2026-11-04 cups.online lets only the session that created a room into
it; everyone else gets a 403 (checked from the node: creator 200, any other
session 403, public room or not). The exit now creates its rooms in one
session and the room list carries it (cups_shared/shared.go); the client and
a restarted exit enter as that session. Both ends are one user then, and
Centrifugo returns each cursor update to its sender too, so an update of our
own user counts as the peer's unless it is one this room just sent.
Lists without a session work as before (and meet the 403).

usage: cups_shared_session.py path/to/core/transport/cupsonline
"""
import os
import shutil
import sys

pkg = sys.argv[1]
here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cups_shared")


def patch(name, pairs):
    path = os.path.join(pkg, name)
    src = open(path, encoding="utf-8").read()
    for old, new, count in pairs:
        if src.count(old) != count:
            sys.exit(f"{path}: expected {count} of {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
        src = src.replace(old, new)
    open(path, "w", encoding="utf-8").write(src)


for f in ("shared.go", "shared_test.go"):
    dst = os.path.join(pkg, f)
    if os.path.exists(dst):
        sys.exit(f"{dst} exists already; the upstream code changed")
    shutil.copy(os.path.join(here, f), dst)

patch("cupsonline.go", [
    ("""	// session keeps the cookies between joins (run's goroutine only).
	session *http.Client
""", """	// session keeps the cookies between joins (run's goroutine only).
	session *http.Client
	// owner is the room's session from the list, and sent the updates this
	// room sent lately (cups_shared_session.py).
	owner string
	sent  sentSet
""", 1),
    ("""func (w *cupsWS) join() error {
	a, err := joinRoom(w.ctx, w.roomUUID, w.session)
""", """func (w *cupsWS) join() error {
	session := w.session
	if session == nil {
		session = newSessionClient(w.owner)
	}
	a, err := joinRoom(w.ctx, w.roomUUID, session)
""", 1),
    ("""	copy(payload[2:], chunk)
""", """	copy(payload[2:], chunk)
	if w.owner != "" {
		w.sent.note(payload)
	}
""", 1),
    ("""	sender, _ := payload["user_uuid"].(string)
	if sender == w.auth().userUUID {
		return
	}
""", """	sender, _ := payload["user_uuid"].(string)
	ownUser := sender == w.auth().userUUID
	if ownUser && w.owner == "" {
		return
	}
""", 1),
    ("""	chunk := decoded[2 : 2+dataLen]
""", """	chunk := decoded[2 : 2+dataLen]
	if ownUser && w.sent.echo(decoded[:2+dataLen]) {
		return // our own update, back from Centrifugo (one session on both ends)
	}
""", 1),
    ("""	stripe  atomic.Pointer[transport.StripedTransport]
""", """	stripe  atomic.Pointer[transport.StripedTransport]
	// owners are the rooms' sessions, from the list or as created
	// (cups_shared_session.py).
	owners []string
""", 1),
    ("""	t.roomIDs, t.striped = parseRooms(rawURL)
""", """	t.roomIDs, t.striped = parseRooms(rawURL)
	t.owners = parseOwners(rawURL, len(t.roomIDs))
""", 1),
    ("""		ws.rpcID.Store(2)
""", """		ws.rpcID.Store(2)
		ws.owner = t.ownerOf(i)
""", 1),
    ("""			slots[i].auth, slots[i].err = joinRoom(t.ctx, id, nil)
""", """			slots[i].auth, slots[i].err = joinRoom(t.ctx, id, t.ownerClient(i))
""", 1),
    ("""	var lastErr error
	for i := 0; i < n; i++ {
		var a *cupsAuth
""", """	var lastErr error
	maker := newRoomMaker()
	for i := 0; i < n; i++ {
		var a *cupsAuth
""", 1),
    ("""			a, lastErr = authorize(ctx, baseURL, nil)
""", """			a, lastErr = authorize(ctx, baseURL, maker.session())
			if lastErr == nil && !maker.made(a) {
				a, lastErr = nil, errSameRoom
			}
""", 1),
    ("""	fmt.Printf("\\n=== COPY THIS TO CLIENT ===\\n")
""", """	t.owners = ownersOf(auths)
	fmt.Printf("\\n=== COPY THIS TO CLIENT ===\\n")
""", 1),
    ("transport.PackRooms(ids, t.striped)", "t.packIDs(ids)", 2),
    ("""	return transport.PackRooms(ids, stripeNewRooms()), nil
""", """	return packList(ids, stripeNewRooms(), ownersOf(auths)), nil
""", 1),
])

patch("fakecups_test.go", [
    ("""	relayed   int
}
""", """	relayed   int
	owners    map[string]string // room -> the user that created it
	ownerOnly bool              // only a room's creator gets its page, as cups.online since 2026-11-04
}
""", 1),
    ("""		room = newUUID()
		f.rooms[room] = true
		f.created++
""", """		room = newUUID()
		f.rooms[room] = true
		f.created++
		if f.owners == nil {
			f.owners = map[string]string{}
		}
		f.owners[room] = user
""", 1),
    ("""	fmt.Fprintf(w, `<html><head>""", """	if f.ownerOnly && f.owners[room] != user {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	fmt.Fprintf(w, `<html><head>""", 1),
])

print("cups shared session: patched", pkg)
