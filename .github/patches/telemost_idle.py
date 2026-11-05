#!/usr/bin/env python3
"""Telemost: an exit without a client sends its filler at 10 frames a second.

Every lane publishes a frame each 1/120 s whether or not there is anything
to send: filler that keeps the SFU forwarding. Each frame is packetized,
encrypted (SRTP) and sent, and the SFU sends it back as a self-view that is
decrypted and decoded too - most of the CPU of an exit with nobody
connected, more after a client's visit left it more lanes. While the exit
has no client (transport.ExitIdle, session_exit_idle.py) the clock runs at
TELEMOST_IDLE_FPS (default 10; 0 keeps 120): still a steady stream, and the
process is woken 10 times a second per lane, not 120. Data waits at most
one idle tick, and the full rate is back the moment the client is heard or
data is to be sent.

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
	idle, slow, idleAt := false, false, time.Time{}
	for range ticker.C {
		if !t.IsRunning() || !t.trackReady.Load() {
			continue
		}
		frames := t.popFrames()
""", 1),
    ("""		if len(frames) == 0 {
			frames = [][]byte{filler(sent)}
		}
		// Spread this tick's frames evenly across the tick instead of""", """		if idleEvery > 0 {
			// While idle the clock itself runs at the idle rate: skipping
			// ticks still woke the process 120 times a second per lane,
			// and on a phone each wakeup costs more than the frame.
			if now := time.Now(); slow || now.Sub(idleAt) > 500*time.Millisecond {
				idle, idleAt = transport.ExitIdle(), now
			}
			switch {
			case idle && !slow && len(frames) == 0:
				ticker.Reset(frameInterval * time.Duration(idleEvery))
				slow = true
			case slow && (!idle || len(frames) > 0):
				ticker.Reset(frameInterval)
				slow = false
			}
		}
		if len(frames) == 0 {
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
