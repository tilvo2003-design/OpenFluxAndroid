package transport

import (
	"encoding/json"
	"testing"
	"time"
)

type idLane struct {
	Transport
	id string
}

func (l *idLane) LaneID() string    { return l.id }
func (l *idLane) IsConnected() bool { return true }

type watchList []string

func (w watchList) Watching() []string { return w }

// The far side's watch list decides which lanes carry data; without a fresh
// one every connected lane does.
func TestWatchListPicksTheLanes(t *testing.T) {
	s := NewStripedTransport([]Transport{&idLane{id: "a"}, &idLane{id: "b"}})
	s.SetWatchSource(watchList{"x"})
	if s.watcher() == nil {
		t.Fatal("no watch source")
	}
	now := time.Now()
	if !s.eligible(0, now) || !s.eligible(1, now) || s.unwatched(1, now) {
		t.Fatal("before any watch list every lane carries")
	}
	body, _ := json.Marshal(map[string][]string{"watch": {"a"}})
	s.onLane(0, append([]byte{stripeCtlMagic}, body...))
	if !s.eligible(0, now) || s.eligible(1, now) || !s.unwatched(1, now) || s.unwatched(0, now) {
		t.Fatal("lane b is not watched and must not carry")
	}
	later := now.Add(6 * time.Second)
	if !s.eligible(1, later) || s.unwatched(1, later) {
		t.Fatal("a stale watch list must not hold lanes back")
	}
}
