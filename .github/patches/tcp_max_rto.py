#!/usr/bin/env python3
"""The tunnel's TCP waits at most 10 s between retransmissions.

The exit's netstack terminates the phone's TCP connections. Its timeout
starts at 3 s (tcpMinRTO) and doubles on every timeout in a row, up to
gVisor's 120 s: after a stall of the carrier (the phone's Avito rooms coming
up one every 12 s, a lane moving to new rooms) a connection waited 24, 48,
96 s for its next try, long after the carrier was back - minutes with nearly
no traffic. Capped at 10 s (OPENFLUX_TCP_MAX_RTO_MS), it tries again soon
after the carrier recovers; 40 retries keep a stalled connection alive about
as long as before (~6 min).

usage: tcp_max_rto.py path/to/core/tunnel/tunnel.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""// TuneTCP sets the tunnel TCP's buffers, retransmission timeout floor and loss
// detection.
func TuneTCP(s *stack.Stack) {
	minRTO := tcpip.TCPMinRTOOption(tcpMinRTO)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &minRTO); err != nil {
		utils.Debugf("[TUNNEL] set min RTO: %v", err)
	}
""", """// tcpMaxRTO caps the doubling of the timeout (patched in by
// tilvo2003-design/OpenFluxAndroid, tcp_max_rto.py): after a stall of the
// carrier the next try comes within this, not after a minute or two.
// OPENFLUX_TCP_MAX_RTO_MS.
var tcpMaxRTO = func() time.Duration {
	if n, err := strconv.Atoi(os.Getenv("OPENFLUX_TCP_MAX_RTO_MS")); err == nil && n > 0 {
		return time.Duration(n) * time.Millisecond
	}
	return 10 * time.Second
}()

// tcpMaxRetries: with the cap the tries come faster; this many keep a stalled
// connection about as long as gVisor's default did.
const tcpMaxRetries = 40

// TuneTCP sets the tunnel TCP's buffers, retransmission timeout floor and loss
// detection.
func TuneTCP(s *stack.Stack) {
	minRTO := tcpip.TCPMinRTOOption(tcpMinRTO)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &minRTO); err != nil {
		utils.Debugf("[TUNNEL] set min RTO: %v", err)
	}
	maxRTO := tcpip.TCPMaxRTOOption(max(tcpMaxRTO, tcpMinRTO))
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &maxRTO); err != nil {
		utils.Debugf("[TUNNEL] set max RTO: %v", err)
	}
	retries := tcpip.TCPMaxRetriesOption(tcpMaxRetries)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &retries); err != nil {
		utils.Debugf("[TUNNEL] set max retries: %v", err)
	}
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("tcp max rto: patched", path)
