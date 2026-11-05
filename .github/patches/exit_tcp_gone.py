#!/usr/bin/env python3
"""Exit: the connections of a client that is gone are closed after 2 min.

The exit's netstack holds the far end of every TCP connection the phone
opened. When the phone goes away its connections stay: each retransmits
what the phone never acknowledged, every 10 s (tcp_max_rto.py) for 40 tries,
about 6 minutes - a steady trickle sent out on every carrier to nobody, and
the internet-side connections held open behind them. Once the exit has
heard its client on no carrier (transport.ExitIdle) for OPENFLUX_EXIT_GONE_S
(default 120; 0 keeps them), its connections are closed.

usage: exit_tcp_gone.py path/to/core/tunnel/tunnel.go
"""
import sys

path = sys.argv[1]
src = open(path, encoding="utf-8").read()
pairs = [
    ("""	var lastRetrans, lastQueueWaits, lastSendRetries uint64

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			stats := t.gvisorStack.Stats()
""", """	var lastRetrans, lastQueueWaits, lastSendRetries uint64
	var goneSince time.Time // exit_tcp_gone.py

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			if t.isExitNode {
				t.dropGoneClient(&goneSince)
			}
			stats := t.gvisorStack.Stats()
"""),
    ("""// logBusiestFlow logs the congestion state of the connection with the
// largest window - the one a download runs on.
""", """// exitGoneAfter is how long an exit holds the connections of a client it
// hears on no carrier (patched in by tilvo2003-design/OpenFluxAndroid,
// exit_tcp_gone.py). OPENFLUX_EXIT_GONE_S, 0 keeps them.
var exitGoneAfter = func() time.Duration {
	if n, err := strconv.Atoi(os.Getenv("OPENFLUX_EXIT_GONE_S")); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	return 2 * time.Minute
}()

// dropGoneClient closes the TCP connections of a client that has been gone
// for exitGoneAfter: they would retransmit to nobody for minutes.
func (t *TCPTunnel) dropGoneClient(since *time.Time) {
	if exitGoneAfter <= 0 || !transport.ExitIdle() {
		*since = time.Time{}
		return
	}
	now := time.Now()
	if since.IsZero() {
		*since = now
		return
	}
	if now.Sub(*since) < exitGoneAfter {
		return
	}
	n := 0
	for _, te := range t.gvisorStack.RegisteredEndpoints() {
		if ep, ok := te.(*tcp.Endpoint); ok {
			ep.Abort()
			n++
		}
	}
	if n > 0 {
		utils.Infof("[TUNNEL] client gone for %s: closed its %d connection(s)", exitGoneAfter+30*time.Second, n)
	}
}

// logBusiestFlow logs the congestion state of the connection with the
// largest window - the one a download runs on.
"""),
]
for old, new in pairs:
    if src.count(old) != 1:
        sys.exit(f"{path}: expected one {old.strip()[:70]!r}, found {src.count(old)}; the upstream code changed")
    src = src.replace(old, new)
open(path, "w", encoding="utf-8").write(src)
print("exit tcp gone: patched", path)
