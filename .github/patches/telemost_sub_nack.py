#!/usr/bin/env python3
"""Telemost: a lost packet is asked for again at most 3 times.

The subscriber is built "without feedback" (TELEMOST_SUB_BWE=0) by giving
webrtc.NewAPI no interceptor registry - but pion v4.2 then registers its
default interceptors itself, NACK generator included, with no limit on how
often one packet is asked for. A packet that reached the SRTP layer but not
the reader (dropped from a full buffer while a client pushed hard) is in
SRTP's replay window, so every resend of it is thrown away as a duplicate
before the NACK generator sees it - and the generator asks again, every
100 ms, forever: once the stream stops its window never moves on. The SFU
dutifully resends. A profile of an exit hours after its client left: a few
thousand packets a second arriving, most of them "duplicated", half a core.

Now the subscriber's interceptors are the same defaults, set up here, with
the generator asking for a packet at most TELEMOST_SUB_NACK_MAX times
(default 3; 0 = no limit, as before). The same limit goes to the NACK
generator TELEMOST_SUB_BWE=1 + TELEMOST_SUB_NACK=1 turns on.

usage: telemost_sub_nack.py path/to/core/transport/yandex/telemost.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""	if envOrDefault("TELEMOST_SUB_BWE", "0") == "0" {
		utils.Debugf("[Telemost] subscriber BWE feedback DISABLED (baseline)")
		return webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine)), nil
	}
""", """	if envOrDefault("TELEMOST_SUB_BWE", "0") == "0" {
		utils.Debugf("[Telemost] subscriber BWE feedback DISABLED (baseline)")
		// pion's defaults, as NewAPI would register them, but with the NACK
		// generator limited (telemost_sub_nack.py).
		ir := &interceptor.Registry{}
		if err := webrtc.ConfigureNackWithOptions(mediaEngine, ir, subNackOptions()); err != nil {
			return nil, err
		}
		if err := webrtc.ConfigureRTCPReports(ir); err != nil {
			return nil, err
		}
		if err := webrtc.ConfigureSimulcastExtensionHeaders(mediaEngine); err != nil {
			return nil, err
		}
		if err := webrtc.ConfigureStatsInterceptor(ir); err != nil {
			return nil, err
		}
		if err := webrtc.ConfigureTWCCSender(mediaEngine, ir); err != nil {
			return nil, err
		}
		return webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine), webrtc.WithInterceptorRegistry(ir)), nil
	}
"""),
    ("""		if err := webrtc.ConfigureNack(mediaEngine, ir); err != nil {
			return nil, err
		}
""", """		if err := webrtc.ConfigureNackWithOptions(mediaEngine, ir, subNackOptions()); err != nil {
			return nil, err
		}
"""),
    ("""// logSelectedPair logs the candidate pair ICE settled on, and every later
""", """// subNackOptions limits how often the subscriber asks for one lost packet
// (TELEMOST_SUB_NACK_MAX, default 3, 0 = no limit): without a limit a packet
// SRTP already took is asked for every 100 ms forever (telemost_sub_nack.py).
func subNackOptions() []pionnack.GeneratorOption {
	n, err := strconv.Atoi(envOrDefault("TELEMOST_SUB_NACK_MAX", "3"))
	if err != nil || n <= 0 || n > 0xffff {
		return nil
	}
	return []pionnack.GeneratorOption{pionnack.GeneratorMaxNacksPerPacket(uint16(n))}
}

// logSelectedPair logs the candidate pair ICE settled on, and every later
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)

imp = '\t"github.com/pion/interceptor"\n'
if src.count(imp) != 1:
    sys.exit(f"{path}: no single pion/interceptor import; the upstream code changed")
src = src.replace(imp, imp + '\tpionnack "github.com/pion/interceptor/pkg/nack"\n')
open(path, "w", encoding="utf-8").write(src)
print("telemost sub nack: patched", path)
