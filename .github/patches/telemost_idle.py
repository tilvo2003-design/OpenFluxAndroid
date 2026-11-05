#!/usr/bin/env python3
"""Telemost: an exit without a client sends its filler at 10 frames a second.

Every lane publishes a frame each 1/120 s whether or not there is anything
to send: filler that keeps the SFU forwarding. Each frame is packetized,
encrypted (SRTP) and sent, and the SFU sends it back as a self-view that is
decrypted and decoded too - most of the CPU of an exit with nobody
connected, more after a client's visit left it more lanes. While the exit
has no client (transport.ExitIdle, session_exit_idle.py) the filler goes out
on every 12th tick (TELEMOST_IDLE_FPS, default 10; 0 keeps 120): still a
steady stream. Data always goes out at once, so a client's handshake is not
held back, and the full rate is back the moment the client is heard.

usage: telemost_idle.py path/to/core/transport/yandex/telemost.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""	sent := 0
	for range ticker.C {
		if !t.IsRunning() || !t.trackReady.Load() {
			continue
		}
		frames := t.popFrames()
""", """	sent := 0
	// An exit without a client sends its filler slower (telemost_idle.py).
	idleEvery := 0
	if t.isExitNode && telemostIdleFPS > 0 && telemostIdleFPS < publishFPS {
		idleEvery = publishFPS / telemostIdleFPS
	}
	idleTick := 0
	idle, idleAt := false, time.Time{}
	for range ticker.C {
		if !t.IsRunning() || !t.trackReady.Load() {
			continue
		}
		frames := t.popFrames()
""", 1),
    ("""		if len(frames) == 0 {
			frames = [][]byte{filler(sent)}
		}
		// Spread this tick's frames evenly across the tick instead of""", """		if len(frames) == 0 {
			if idleEvery > 0 {
				if now := time.Now(); now.Sub(idleAt) > 500*time.Millisecond {
					idle, idleAt = transport.ExitIdle(), now
				}
				if idle {
					if idleTick++; idleTick%idleEvery != 0 {
						continue
					}
				}
			}
			frames = [][]byte{filler(sent)}
		}
		// Spread this tick's frames evenly across the tick instead of""", 1),
    ("""var publishFPS = envIntDefault("TELEMOST_FPS", 120)
""", """var publishFPS = envIntDefault("TELEMOST_FPS", 120)

// telemostIdleFPS is the filler rate of an exit without a client
// (telemost_idle.py); TELEMOST_IDLE_FPS=0 keeps publishFPS.
var telemostIdleFPS = func() int {
	if v, ok := os.LookupEnv("TELEMOST_IDLE_FPS"); ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return 10
}()
""", 1),
]
for old, new, count in pairs:
    if src.count(old) != count:
        sys.exit(f"{path}: expected {count} of {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("telemost idle: patched", path)
