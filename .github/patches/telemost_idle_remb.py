#!/usr/bin/env python3
"""Telemost: an exit without a client tells the SFU its downlink is small.

Each video track a lane receives starts a loop that tells the SFU, once a
second, that this subscriber can take 100 Mbit/s (TELEMOST_SUB_REMB_KBPS):
it opens the SFU's forward rate. The loop never ended - not with its track,
not when the client left - so an exit whose client had come and gone kept
telling the SFU about 100 Mbit/s on every track it had ever watched, with
nothing to forward. The profile of such an exit, hours after its client
left: thousands of packets a second arriving on the subscribers, most of
them repeats of packets already received, half a core spent taking them in
and the heap refilled by them as fast as it was handed back. Before the
first client (no track, no loop) the same exit sat near zero.

Now the loop ends with its track, and while the exit is idle
(transport.ExitIdle) it reports TELEMOST_IDLE_REMB_KBPS (default 100; 0
keeps the full value). It looks four times a second, so the full value is
back within a quarter second of the client being heard.

usage: telemost_idle_remb.py path/to/core/transport/yandex/telemost.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""func (t *TelemostTransport) sendREMBLoop(mediaSSRC webrtc.SSRC, bps uint64) {
	pc := t.subscriberPC
	if pc == nil {
		return
	}
	utils.Debugf("[Telemost] REMB sender started: %d bps ssrc=%d [%s]",
		bps, uint32(mediaSSRC), roleStr(t.isExitNode))
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for t.IsRunning() {
		if err := pc.WriteRTCP([]rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{
			Bitrate: float32(bps),
			SSRCs:   []uint32{uint32(mediaSSRC)},
		}}); err != nil {
			utils.Debugf("[Telemost] REMB WriteRTCP: %v [%s]", err, roleStr(t.isExitNode))
			return
		}
		<-ticker.C
	}
}
""", """func (t *TelemostTransport) sendREMBLoop(mediaSSRC webrtc.SSRC, bps uint64, stop <-chan struct{}) {
	pc := t.subscriberPC
	if pc == nil {
		return
	}
	utils.Debugf("[Telemost] REMB sender started: %d bps ssrc=%d [%s]",
		bps, uint32(mediaSSRC), roleStr(t.isExitNode))
	// An idle exit reports telemostIdleREMB instead (telemost_idle_remb.py),
	// and the loop ends with its track.
	ticker := time.NewTicker(time.Second / 4)
	defer ticker.Stop()
	var sent uint64
	var sentAt time.Time
	for t.IsRunning() {
		rate := bps
		if t.isExitNode && telemostIdleREMB > 0 && telemostIdleREMB < bps && transport.ExitIdle() {
			rate = telemostIdleREMB
		}
		if rate != sent || time.Since(sentAt) >= time.Second {
			if err := pc.WriteRTCP([]rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{
				Bitrate: float32(rate),
				SSRCs:   []uint32{uint32(mediaSSRC)},
			}}); err != nil {
				utils.Debugf("[Telemost] REMB WriteRTCP: %v [%s]", err, roleStr(t.isExitNode))
				return
			}
			if sent != 0 && rate != sent {
				utils.Debugf("[Telemost] lane %d REMB ssrc=%d: %d -> %d kbit/s [%s]",
					t.laneIndex(), uint32(mediaSSRC), sent/1000, rate/1000, roleStr(t.isExitNode))
			}
			sent, sentAt = rate, time.Now()
		}
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
	}
}

// telemostIdleREMB is the downlink an idle exit reports (bit/s,
// telemost_idle_remb.py); TELEMOST_IDLE_REMB_KBPS=0 keeps the full value.
var telemostIdleREMB = func() uint64 {
	if v, ok := os.LookupEnv("TELEMOST_IDLE_REMB_KBPS"); ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return uint64(n) * 1000
		}
	}
	return 100 * 1000
}()
"""),
    ("""		if kbps, _ := strconv.Atoi(v); kbps > 0 {
			go t.sendREMBLoop(track.SSRC(), uint64(kbps)*1000)
		}
""", """		if kbps, _ := strconv.Atoi(v); kbps > 0 {
			rembStop := make(chan struct{})
			defer close(rembStop)
			go t.sendREMBLoop(track.SSRC(), uint64(kbps)*1000, rembStop)
		}
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("telemost idle remb: patched", path)
