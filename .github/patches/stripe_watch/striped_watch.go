package transport

// Watch lists for any striped carrier (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/stripe_watch_source.py).
//
// A Telemost stripe tells the far side which lanes it hears, so the far side
// sends only into those. A carrier of rooms (Avito) needs the same: its lane
// counts as up once this side's send room is, whether or not the far side
// reads the room yet - a phone brings its rooms up one every 12 s, and for
// minutes the exit's pieces went into rooms nobody read, were resent seconds
// later and the tunnel's TCP crawled.

import "time"

// SetWatchSource makes the stripe tell the far side, every second, which of
// its lanes this side hears (ws), when no LaneSource does. Call before Start.
func (s *StripedTransport) SetWatchSource(ws WatchSource) { s.watchFrom = ws }

func (s *StripedTransport) watcher() WatchSource {
	if ws, ok := s.src.(WatchSource); ok {
		return ws
	}
	return s.watchFrom
}

// unwatched: the far side said lately what it hears, and lane i is not in it.
func (s *StripedTransport) unwatched(i int, now time.Time) bool {
	lanes, _ := s.snapshotLanes()
	if i >= len(lanes) {
		return false
	}
	id, ok := lanes[i].(LaneIDer)
	if !ok {
		return false
	}
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.watched == nil || now.Sub(s.watchAt) > 5*time.Second {
		return false
	}
	return !s.watched[id.LaneID()]
}
