package cupsonline

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

func TestPackListCarriesSessions(t *testing.T) {
	ids := []string{"a", "b", "c"}
	one := packList(ids, true, []string{"sessionid=s1", "sessionid=s1", "sessionid=s1"})
	got, striped, err := transport.UnpackRooms(one)
	if err != nil || !striped || strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("UnpackRooms: %v %v %v", got, striped, err)
	}
	if o := parseOwners(one, 3); strings.Join(o, ",") != "sessionid=s1,sessionid=s1,sessionid=s1" {
		t.Fatalf("one session for all: %v", o)
	}
	each := packList(ids, true, []string{"sessionid=1", "sessionid=2", "sessionid=3"})
	if o := parseOwners("https://x/?rooms="+each, 3); strings.Join(o, ",") != "sessionid=1,sessionid=2,sessionid=3" {
		t.Fatalf("a session per room: %v", o)
	}
	if packList(ids, true, []string{"sessionid=1", "", "sessionid=3"}) != transport.PackRooms(ids, true) {
		t.Fatal("a list with a room missing its session must not carry sessions")
	}
	if o := parseOwners(transport.PackRooms(ids, true), 3); o != nil {
		t.Fatalf("an old list gave sessions %v", o)
	}
}

func TestEchoIsToldByContent(t *testing.T) {
	var s sentSet
	s.note([]byte("mine"))
	if s.echo([]byte("theirs")) {
		t.Fatal("the peer's update taken for an echo")
	}
	if !s.echo([]byte("mine")) {
		t.Fatal("our own update not recognised")
	}
	if s.echo([]byte("mine")) {
		t.Fatal("an echo is recognised once: the peer may send the same bytes later")
	}
}

// cups.online lets only a room's creator in: both ends go in as the exit's
// session, and carry a stream both ways without taking their own updates for
// the peer's.
func TestOneSessionCarriesBothWays(t *testing.T) {
	f := newFakeCups(t)
	f.locked(func() { f.ownerOnly = true })
	cfg := fastConfig()
	exit, client := startStripedPair(t, cfg)
	if o := parseOwners(exit.RoomList(), len(exit.wss)); len(o) == 0 || o[0] == "" {
		t.Fatalf("the exit's list %q carries no session", exit.RoomList())
	}
	const n = 200
	for _, dir := range []struct {
		name     string
		from, to *peer
	}{{"up", client, exit}, {"down", exit, client}} {
		t.Run(dir.name, func(t *testing.T) {
			var got atomic.Int64
			done := make(chan struct{})
			go func() {
				defer close(done)
				expectInOrderCounting(t, dir.to, n, 20*time.Second, &got)
			}()
			sendWindowed(t, dir.from, n, &got, done)
			<-done
			time.Sleep(200 * time.Millisecond)
			if k := len(dir.from.got); k > 0 {
				t.Fatalf("the sender got %d of its own frames back as the peer's", k)
			}
		})
	}
}

// A restarted exit enters its rooms again as their session; a client with a
// list from before (no session) is turned away.
func TestRestartedExitEntersItsRooms(t *testing.T) {
	f := newFakeCups(t)
	f.locked(func() { f.ownerOnly = true })
	t.Setenv("CUPS_STRIPE", "")
	cfg := fastConfig()
	first := mustStart(t, "", false, cfg)
	list := first.RoomList()
	ids, _, _ := transport.UnpackRooms(list)
	first.Stop()

	again := mustStart(t, list, false, cfg)
	waitFor(t, 5*time.Second, "the restarted exit in all its rooms", func() bool { return allConnected(again) })
	if again.RoomList() != list {
		t.Fatalf("the restarted exit changed its list:\n%s\n%s", list, again.RoomList())
	}
	if created := f.roomsCreated(); created != len(ids) {
		t.Fatalf("%d rooms created, want only the first %d", created, len(ids))
	}
	if _, err := startPeer(t, transport.PackRooms(ids, true), true, cfg); err == nil {
		t.Fatal("a client without the session got into the rooms")
	}
}
