package avito

import (
	"testing"
	"time"
)

func (f *fakeAvito) openConns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// The rooms share connections: two lanes (four rooms) to one, plus the
// control room's own.
func TestRoomsShareConnections(t *testing.T) {
	f := newFakeAvito(t, time.Millisecond)
	cfg := fastConfig() // 3 lanes: 6 rooms a side
	exit, client := startStripedPair(t, cfg)
	if n := f.openConns(); n != 6 {
		t.Fatalf("%d connections for 2 x 6 rooms, want 6 (2 shared + the control room's, a side)", n)
	}
	if !allConnected(exit) || !allConnected(client) {
		t.Fatal("rooms down")
	}
}

func TestOneConnectionEachWhenAsked(t *testing.T) {
	t.Setenv("AVITO_MUX_ROOMS", "1")
	f := newFakeAvito(t, time.Millisecond)
	startStripedPair(t, fastConfig())
	if n := f.openConns(); n != 14 {
		t.Fatalf("%d connections, want 14 (one per room and the control rooms)", n)
	}
}

func TestDocOf(t *testing.T) {
	for _, c := range []struct {
		raw   string
		doc   string
		unsub bool
	}{
		{`{"a":"op","c":"docs","d":"room-1","v":3,"src":"x","seq":2,"op":[{"d":10},"abc"]}`, "room-1", false},
		{`{"a":"us","c":"docs","d":"room-2"}`, "room-2", true},
		{`{"op":[{"d":5}],"a":"op","d":"room-3"}`, "room-3", false},
		{`{"a":"init","id":"x"}`, "", false},
	} {
		doc, unsub := docOf([]byte(c.raw))
		if doc != c.doc || unsub != c.unsub {
			t.Errorf("docOf(%s) = %q, %v", c.raw, doc, unsub)
		}
	}
}
