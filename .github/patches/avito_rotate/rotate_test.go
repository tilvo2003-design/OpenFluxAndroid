package avito

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

// No spare rooms in the tests unless one asks: a reserve being made up when
// a test ends would still be calling the fake Avito the test took down.
func init() { sparePairs = 0 }

func (f *fakeAvito) spend(room string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.spent == nil {
		f.spent = map[string]bool{}
	}
	f.spent[room] = true
}

func (f *fakeAvito) logText(room string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d := f.docs[room]; d != nil {
		return d.text
	}
	return ""
}

func fastRotation(t *testing.T) {
	oldPause, oldWait := opLimitPause, ctlStartWait
	opLimitPause, ctlStartWait = time.Millisecond, 5*time.Second
	t.Cleanup(func() { opLimitPause, ctlStartWait = oldPause, oldWait })
}

func TestRoomListWithControlRoom(t *testing.T) {
	tr := &AvitoTransport{striped: true}
	tr.rot.ctlID = "ctl-room"
	packed := tr.packRooms([]string{"a", "b", "c", "d"})
	ids, striped, err := transport.UnpackRooms(packed)
	if err != nil || !striped || strings.Join(ids, ",") != "a,b,c,d" {
		t.Fatalf("UnpackRooms: %v %v %v", ids, striped, err)
	}
	if got := parseCtl(packed); got != "ctl-room" {
		t.Fatalf("parseCtl = %q", got)
	}
	if got := parseCtl("https://x/?rooms=" + packed); got != "ctl-room" {
		t.Fatalf("parseCtl(?rooms=) = %q", got)
	}
	if got := parseCtl(transport.PackRooms([]string{"a", "b"}, true)); got != "" {
		t.Fatalf("a list without a control room gave %q", got)
	}
}

func TestApplyText(t *testing.T) {
	for _, c := range []struct {
		text string
		op   []interface{}
		want string
		ok   bool
	}{
		{"", []interface{}{"R 1 x\n"}, "R 1 x\n", true},
		{"ab", []interface{}{float64(2), "c"}, "abc", true},
		{"abcd", []interface{}{float64(1), map[string]interface{}{"d": float64(2)}, "X"}, "aXd", true},
		{"abcd", []interface{}{map[string]interface{}{"d": "ab"}}, "cd", true},
		{"ab", []interface{}{float64(3), "c"}, "ab", false},
		{"ab", []interface{}{map[string]interface{}{"d": float64(5)}}, "ab", false},
		{"я😀b", []interface{}{float64(3), "c"}, "я😀cb", true}, // UTF-16 units, as ShareDB counts
	} {
		got, ok := applyText(c.text, c.op)
		if got != c.want || ok != c.ok {
			t.Errorf("applyText(%q, %v) = %q, %v; want %q, %v", c.text, c.op, got, ok, c.want, c.ok)
		}
	}
}

func TestParseCtlLog(t *testing.T) {
	l1 := transport.PackRooms([]string{"a", "b"}, true)
	l2 := transport.PackRooms([]string{"c", "b"}, true)
	text := "R 1 " + l1 + "\nS a\ngarbage\nR 2 " + l2 + "\nR x " + l1 + "\nS b\n"
	gen, ids, spent, used := parseCtlLog(text + "U c 100\nU c 300\nU b x\n")
	if gen != 2 || strings.Join(ids, ",") != "c,b" || strings.Join(spent, ",") != "a,b" {
		t.Fatalf("got %d %v %v", gen, ids, spent)
	}
	if len(used) != 1 || used["c"] != 300 {
		t.Fatalf("used %v, want c: 300", used)
	}
	// Lines merged in another order mean the same.
	gen2, ids2, _, _ := parseCtlLog("R 2 " + l2 + "\nS a\nR 1 " + l1 + "\n")
	if gen2 != 2 || strings.Join(ids2, ",") != "c,b" {
		t.Fatalf("reordered: %d %v", gen2, ids2)
	}
}

// streamDown sends exit->client frames until stop closes.
func streamDown(t *testing.T, exit *peer, stop chan struct{}) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		payload := bytes.Repeat([]byte{7}, 20000)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = exit.Send(payload)
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return done
}

func roomsOf(p *peer) []string {
	p.rot.mu.Lock()
	defer p.rot.mu.Unlock()
	return append([]string(nil), p.rot.ids...)
}

func sameRooms(a, b []string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

// laneRoomsMoved: every room of both sides is on the rooms of the list.
func roomsFollow(p *peer, ids []string) bool {
	p.rot.mu.Lock()
	byPos := p.rot.byPos
	p.rot.mu.Unlock()
	if len(byPos) != len(ids) {
		return false
	}
	for i, w := range byPos {
		if w.room() != ids[i] || !w.connected.Load() {
			return false
		}
	}
	return true
}

// isFiller: a frame of streamDown or streamUp, which may still be on the way.
func isFiller(b []byte) bool {
	return len(b) == 20000 && (b[0] == 7 || b[0] == 9) && bytes.Count(b, b[:1]) == len(b)
}

// expectFrames is expectInOrder past the filler frames.
func expectFrames(t *testing.T, p *peer, n int, within time.Duration, got *atomic.Int64) {
	deadline := time.After(within)
	for i := 0; i < n; {
		select {
		case b := <-p.got:
			if isFiller(b) {
				continue
			}
			if !bytes.Equal(b, seqFrame(i)) {
				idx := -1
				if len(b) >= 4 {
					idx = int(binary.BigEndian.Uint32(b))
				}
				t.Errorf("frame %d: got frame %d (%d bytes)", i, idx, len(b))
				return
			}
			got.Add(int64(len(b)))
			i++
		case <-deadline:
			t.Errorf("only %d of %d frames arrived", i, n)
			return
		}
	}
}

func checkStillCarries(t *testing.T, exit, client *peer) {
	t.Helper()
	// The streams' backlog first: frames waiting behind it would be dropped
	// by the stripe's queue management, not lost by the move.
	for quiet := 0; quiet < 2; {
		select {
		case <-client.got:
			quiet = 0
		case <-exit.got:
			quiet = 0
		case <-time.After(500 * time.Millisecond):
			quiet++
		}
	}
	for _, dir := range []struct {
		name     string
		from, to *peer
	}{{"down", exit, client}, {"up", client, exit}} {
		const n = 60
		var got atomic.Int64
		done := make(chan struct{})
		go func() { defer close(done); expectFrames(t, dir.to, n, 30*time.Second, &got) }()
		sendWindowed(t, dir.from, n, 256<<10, &got, done)
		<-done
		if t.Failed() {
			t.Fatalf("%s: frames lost after the rooms moved", dir.name)
		}
	}
}

func TestSpentLaneMovesToNewRooms(t *testing.T) {
	fastRotation(t)
	f := newFakeAvito(t, time.Millisecond)
	cfg := fastConfig()
	cfg.NumRooms = 2
	exit, client := startStripedPair(t, cfg)
	if exit.rot.ctlID == "" || client.rot.ctlID != exit.rot.ctlID {
		t.Fatalf("control room: exit %q, client %q", exit.rot.ctlID, client.rot.ctlID)
	}
	first := roomsOf(exit)
	half := len(first) / 2

	// The exit's send room of lane 1 is spent.
	f.spend(first[0])
	stop := make(chan struct{})
	streaming := streamDown(t, exit, stop)
	waitFor(t, 20*time.Second, "lane 1 on new rooms at both ends", func() bool {
		now := roomsOf(exit)
		return now[0] != first[0] && now[half] != first[half] && sameRooms(roomsOf(client), now) &&
			roomsFollow(exit, now) && roomsFollow(client, now)
	})
	close(stop)
	<-streaming
	now := roomsOf(exit)
	for i := range now {
		if i != 0 && i != half && now[i] != first[i] {
			t.Errorf("room %d of another lane moved: %s -> %s", i, first[i], now[i])
		}
	}
	if got := exit.RoomList(); !strings.Contains(fmt.Sprint(mustUnpack(t, got)), now[0]) || parseCtl(got) != exit.rot.ctlID {
		t.Errorf("RoomList %q does not name the new rooms and the control room", got)
	}
	checkStillCarries(t, exit, client)

	// The client's send room of lane 2 is spent: the client reports it, the exit replaces the lane.
	before := roomsOf(exit)
	f.spend(before[half+1])
	stopUp := make(chan struct{})
	upDone := make(chan struct{})
	go func() {
		defer close(upDone)
		payload := bytes.Repeat([]byte{9}, 20000)
		for {
			select {
			case <-stopUp:
				return
			default:
			}
			_ = client.Send(payload)
			time.Sleep(10 * time.Millisecond)
		}
	}()
	waitFor(t, 20*time.Second, "lane 2 on new rooms after the client's report", func() bool {
		now := roomsOf(exit)
		return now[1] != before[1] && now[half+1] != before[half+1] && sameRooms(roomsOf(client), now) &&
			roomsFollow(exit, now) && roomsFollow(client, now)
	})
	close(stopUp)
	<-upDone
	if log := f.logText(exit.rot.ctlID); !strings.Contains(log, "S "+before[half+1]) {
		t.Errorf("the client's report is not in the log:\n%s", log)
	}
	checkStillCarries(t, exit, client)
}

func mustUnpack(t *testing.T, list string) []string {
	ids, _, err := transport.UnpackRooms(list)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// A restarted exit, and a client, given the first list come up on the rooms
// the log names.
func TestStartFollowsTheLog(t *testing.T) {
	fastRotation(t)
	f := newFakeAvito(t, time.Millisecond)
	cfg := fastConfig()
	cfg.NumRooms = 2
	exit := mustStart(t, "", false, cfg)
	link := exit.RoomList()
	first := roomsOf(exit)
	f.spend(first[1])
	exit.replaceLaneOf(first[1], "test")
	waitFor(t, 10*time.Second, "lane 2 replaced", func() bool { return roomsOf(exit)[1] != first[1] })
	want := roomsOf(exit)
	waitFor(t, 10*time.Second, "the new list in the log", func() bool {
		gen, ids, _, _ := parseCtlLog(f.logText(exit.rot.ctlID))
		return gen == 1 && sameRooms(ids, want)
	})
	exit.Stop()

	again := mustStart(t, link, false, cfg)
	if got := roomsOf(again); !sameRooms(got, want) {
		t.Fatalf("restarted exit on %v, want %v", got, want)
	}
	client := mustStart(t, link, true, cfg)
	if got := roomsOf(client); !sameRooms(got, want) {
		t.Fatalf("client on %v, want %v", got, want)
	}
	waitFor(t, 10*time.Second, "both up on the rooms of the log", func() bool {
		return roomsFollow(again, want) && roomsFollow(client, want)
	})
	var got atomic.Int64
	done := make(chan struct{})
	go func() { defer close(done); expectInOrder(t, client, 30, 20*time.Second, &got) }()
	sendWindowed(t, again, 30, 256<<10, &got, done)
	<-done
}

// The appends of both ends merge in the log, whatever their order.
func TestControlLogAppendsFromBothEnds(t *testing.T) {
	fastRotation(t)
	f := newFakeAvito(t, 3*time.Millisecond)
	cfg := fastConfig()
	cfg.NumRooms = 1
	exit, client := startStripedPair(t, cfg)
	for i := 0; i < 5; i++ {
		exit.rot.ctl.append(fmt.Sprintf("X exit-%d", i))
		client.rot.ctl.append(fmt.Sprintf("X client-%d", i))
	}
	waitFor(t, 10*time.Second, "all ten lines in the log", func() bool {
		log := f.logText(exit.rot.ctlID)
		for i := 0; i < 5; i++ {
			if !strings.Contains(log, fmt.Sprintf("X exit-%d\n", i)) || !strings.Contains(log, fmt.Sprintf("X client-%d\n", i)) {
				return false
			}
		}
		return true
	})
}

// The exit gives a lane new rooms before Avito would refuse them, while the
// stream goes on whole (the lanes keep moving during the check too).
func TestLanesGetNewRoomsBeforeTheirBudgetRunsOut(t *testing.T) {
	r := &rotation{budgetLow: 6 << 20, budgetHigh: 12 << 20}
	if b := r.laneBudget(0, 8); b != 6<<20 || r.laneBudget(7, 8) != 12<<20 || r.laneBudget(3, 8) <= b {
		t.Fatalf("budgets %d..%d not staggered", r.laneBudget(0, 8), r.laneBudget(7, 8))
	}
	fastRotation(t)
	oldLow, oldHigh, oldEvery, oldQuick := budgetLow, budgetHigh, budgetEvery, laneQuickBytes
	budgetLow, budgetHigh, budgetEvery, laneQuickBytes = 300<<10, 900<<10, 100*time.Millisecond, 1
	t.Cleanup(func() { budgetLow, budgetHigh, budgetEvery, laneQuickBytes = oldLow, oldHigh, oldEvery, oldQuick })
	newFakeAvito(t, time.Millisecond)
	cfg := fastConfig()
	cfg.NumRooms = 2
	exit, client := startStripedPair(t, cfg)
	first := roomsOf(exit)
	stop := make(chan struct{})
	streaming := streamDown(t, exit, stop)
	waitFor(t, 30*time.Second, "both lanes on new rooms at both ends", func() bool {
		now := roomsOf(exit)
		return now[0] != first[0] && now[1] != first[1] && sameRooms(roomsOf(client), now) &&
			roomsFollow(exit, now) && roomsFollow(client, now)
	})
	close(stop)
	<-streaming
	checkStillCarries(t, exit, client)
	// The lanes go on moving for a moment: let them finish before the fake goes.
	waitFor(t, 30*time.Second, "the last replacements done", func() bool {
		exit.rot.mu.Lock()
		defer exit.rot.mu.Unlock()
		return len(exit.rot.busy) == 0
	})
}

// A lane whose rooms run out takes a spare pair at once, and the reserve is
// made up again.
func TestSparePairTakesOver(t *testing.T) {
	fastRotation(t)
	old := sparePairs
	sparePairs = 1
	t.Cleanup(func() { sparePairs = old })
	newFakeAvito(t, time.Millisecond)
	cfg := fastConfig()
	cfg.NumRooms = 2
	exit, client := startStripedPair(t, cfg)
	var spare [2]string
	waitFor(t, 10*time.Second, "a spare pair", func() bool {
		exit.rot.mu.Lock()
		defer exit.rot.mu.Unlock()
		if len(exit.rot.spares) == 1 {
			spare = exit.rot.spares[0]
			return true
		}
		return false
	})
	first := roomsOf(exit)
	exit.replaceLaneOf(first[0], "test")
	waitFor(t, 10*time.Second, "lane 1 on the spare pair at both ends", func() bool {
		now := roomsOf(exit)
		return now[0] == spare[0] && now[len(now)/2] == spare[1] && sameRooms(roomsOf(client), now) &&
			roomsFollow(exit, now) && roomsFollow(client, now)
	})
	waitFor(t, 10*time.Second, "the reserve made up", func() bool {
		exit.rot.mu.Lock()
		defer exit.rot.mu.Unlock()
		return len(exit.rot.spares) == 1 && exit.rot.spares[0] != spare
	})
	checkStillCarries(t, exit, client)
}

// What a room carried is in the log, and a restarted exit counts on from it.
func TestRoomUseSurvivesARestart(t *testing.T) {
	fastRotation(t)
	oldLow, oldHigh := budgetLow, budgetHigh
	budgetLow, budgetHigh = 100<<20, 200<<20 // no replacement during the test
	t.Cleanup(func() { budgetLow, budgetHigh = oldLow, oldHigh })
	f := newFakeAvito(t, time.Millisecond)
	cfg := fastConfig()
	cfg.NumRooms = 2
	exit := mustStart(t, "", false, cfg)
	link := exit.RoomList()
	room := roomsOf(exit)[0]
	exit.rot.ctl.append(fmt.Sprintf("U %s %d", room, 7<<20))
	waitFor(t, 10*time.Second, "the use in the log", func() bool {
		return strings.Contains(f.logText(exit.rot.ctlID), "U "+room+" ")
	})
	exit.Stop()
	again := mustStart(t, link, false, cfg)
	if u := again.txWss[0].used(); u < 7<<20 {
		t.Fatalf("the restarted exit counts %d bytes for room %s, want at least 7 MB", u, room[:8])
	}
	waitFor(t, 10*time.Second, "the restarted exit's rooms up", func() bool { return allConnected(again) })
}
