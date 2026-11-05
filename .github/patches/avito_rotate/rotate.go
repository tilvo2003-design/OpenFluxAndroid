package avito

// Room rotation (patched in by tilvo2003-design/OpenFluxAndroid,
// .github/patches/avito_rotate.py).
//
// Avito takes about 20 MB of data into a room, then refuses every op in it
// for good (ERR_ROOM_OPERATION_LIMIT). A room list with a control room ("ctl"
// in its JSON; older builds ignore it) lets the two ends give a lane new rooms
// without a new link. The control room's document is a log of lines that both
// ends append to and nobody rewrites, so appends made at the same time merge:
//
//	R <generation> <packed room list>   the exit: the rooms in use from now on
//	S <room id>                         the client: Avito refuses its data in this room
//
// The exit gives the lane of a spent room - spent on its side or reported by
// the client - two new rooms and appends the new list; both ends move that
// lane over, the other lanes carry on. Both ends read the log when they start,
// so a restarted exit or a client that was away comes back on the rooms in
// use, with the link it always had.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

var (
	// ctlStartWait is how long Start waits for the control room before it
	// goes on with the rooms of the link.
	ctlStartWait = 30 * time.Second
	// A lane whose rooms took less than laneQuickBytes before Avito refused
	// them waits before it gets more: lanePauseMin, doubling up to
	// lanePauseMax. Every new room costs two of Avito's handshakes (join,
	// connect), shared by every node of the address, so rooms that die for
	// another reason than their data must not starve the others.
	laneQuickBytes uint64 = 2 << 20
	lanePauseMin          = time.Minute
	lanePauseMax          = 30 * time.Minute

	// The exit replaces a lane's rooms before Avito refuses them: once either
	// room took laneBudget(lane) bytes. Avito stops at ~19.5 MB of ops in a
	// room, ~14 MB of ours (they go as base64), and the stripe fills its lanes
	// evenly, so they would all run out together and the replacements queue
	// up at Avito's handshake budget for minutes; the budgets run from
	// budgetLow (lane 1) to budgetHigh (the last), so the lanes take turns.
	// AVITO_BUDGET_MB sets budgetHigh (budgetLow is half of it).
	budgetLow, budgetHigh = budgetFromEnv()
	budgetEvery           = 5 * time.Second
)

func budgetFromEnv() (uint64, uint64) {
	hi := uint64(envKB("AVITO_BUDGET_MB", 12)) << 20
	return hi / 2, hi
}

// laneBudget is how much lane may take into one of its rooms.
func (r *rotation) laneBudget(lane, lanes int) uint64 {
	if lanes <= 1 {
		return r.budgetHigh
	}
	return r.budgetLow + (r.budgetHigh-r.budgetLow)*uint64(lane)/uint64(lanes-1)
}

var errMoved = errors.New("avito: the room was replaced")

// rotateOn: AVITO_ROTATE=0 makes new lists without a control room.
func rotateOn() bool { return os.Getenv("AVITO_ROTATE") != "0" }

// roomListCtl is the striped room list (transport.PackRooms) with the
// control room added.
type roomListCtl struct {
	V     int      `json:"v"`
	Rooms []string `json:"rooms"`
	Ctl   string   `json:"ctl,omitempty"`
}

// parseCtl returns the control room of --url's list, "" when it has none.
func parseCtl(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	cands := []string{rawURL}
	if u, err := url.Parse(rawURL); err == nil {
		if p := u.Query().Get("rooms"); p != "" {
			cands = append([]string{p}, cands...)
		}
	}
	for _, c := range cands {
		raw, err := base64.RawURLEncoding.DecodeString(c)
		if err != nil || len(raw) == 0 || raw[0] != '{' {
			continue
		}
		var v roomListCtl
		if json.Unmarshal(raw, &v) == nil && v.Ctl != "" {
			return v.Ctl
		}
	}
	return ""
}

// packRooms is transport.PackRooms with the control room, when there is one.
func (t *AvitoTransport) packRooms(ids []string) string {
	if t.rot.ctlID == "" || !t.striped {
		return transport.PackRooms(ids, t.striped)
	}
	raw, _ := json.Marshal(roomListCtl{V: 2, Rooms: ids, Ctl: t.rot.ctlID})
	return base64.RawURLEncoding.EncodeToString(raw)
}

type rotation struct {
	ctlID string
	ctl   *ctlLog

	mu      sync.Mutex
	gen     int
	ids     []string   // the rooms in use, in list order
	byPos   []*avitoWS // this side's room at each list position, once Start made them
	busy    map[int]bool
	movedAt map[int]time.Time
	pause   map[int]time.Duration
	budget  bool // budgetLoop runs

	// The settings above as Start found them.
	budgetLow, budgetHigh, quickBytes uint64
	budgetEvery                       time.Duration
	told                              map[string]bool // client: rooms reported spent

	create sync.Mutex // the exit makes one lane's rooms at a time
}

// makeCtlRoom creates the control room for a new list of n rooms.
func (t *AvitoTransport) makeCtlRoom(n int) {
	if !rotateOn() || !t.striped || n < 2 || n%2 != 0 {
		return
	}
	for attempt := 0; attempt < 5; attempt++ {
		id, err := createRoom(t.ctx, t.cookies)
		if err == nil {
			if jerr := registerRoom(t.ctx, id); jerr != nil && t.ctx.Err() == nil {
				utils.Debugf("[AVITO] join control room %s: %v (joined later)", id[:8], jerr)
			}
			t.rot.ctlID = id
			utils.Infof("[AVITO] created control room %s: a lane gets new rooms when Avito stops taking data in them", id[:8])
			return
		}
		if t.ctx.Err() != nil {
			return
		}
		utils.Debugf("[AVITO] control room attempt %d failed: %v", attempt+1, err)
		if !sleepCtx(t.ctx, t.config.RoomCreatePause*time.Duration(1<<uint(attempt))) {
			return
		}
	}
	utils.Infof("[AVITO] no control room: spent rooms will not be replaced")
}

// startRotation reads the control room, when the list has one, and returns
// the rooms to use: the latest the log names, else ids.
func (t *AvitoTransport) startRotation(ids []string) []string {
	r := &t.rot
	if r.ctlID == "" || !t.striped || len(ids) < 2 || len(ids)%2 != 0 {
		return ids
	}
	r.mu.Lock()
	r.ids = append([]string(nil), ids...)
	r.budgetLow, r.budgetHigh, r.quickBytes, r.budgetEvery = budgetLow, budgetHigh, laneQuickBytes, budgetEvery
	r.mu.Unlock()
	r.ctl = newCtlLog(t, r.ctlID, t.onCtlText)
	go r.ctl.run()
	timer := time.NewTimer(ctlStartWait)
	defer timer.Stop()
	select {
	case <-r.ctl.ready:
	case <-timer.C:
		utils.Infof("[AVITO] control room %s not reached in %v: starting on the link's rooms", r.ctlID[:8], ctlStartWait)
	case <-t.ctx.Done():
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

// wireRotation hooks the rooms Start made into the rotation.
func (t *AvitoTransport) wireRotation() {
	r := &t.rot
	if r.ctl == nil {
		return
	}
	byPos := make([]*avitoWS, 0, len(t.txWss)+len(t.rxWss))
	if t.isClient {
		byPos = append(append(byPos, t.rxWss...), t.txWss...)
	} else {
		byPos = append(append(byPos, t.txWss...), t.rxWss...)
	}
	// Only the rooms a side writes to can be found spent there.
	for _, w := range t.txWss {
		w.onSpent = t.roomSpent
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(byPos) != len(r.ids) {
		return
	}
	// The log may have moved on while Start made the rooms.
	for p, w := range byPos {
		if w.roomID != r.ids[p] {
			id := r.ids[p]
			w.next.Store(&id)
		}
	}
	r.byPos = byPos
	if !t.isClient && !r.budget {
		r.budget = true
		go t.budgetLoop()
	}
}

// budgetLoop (exit) replaces a lane's rooms once one of them took its budget.
func (t *AvitoTransport) budgetLoop() {
	tick := time.NewTicker(t.rot.budgetEvery)
	defer tick.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-tick.C:
		}
		r := &t.rot
		r.mu.Lock()
		byPos := r.byPos
		r.mu.Unlock()
		half := len(byPos) / 2
		for lane := 0; lane < half; lane++ {
			used := max(byPos[lane].used(), byPos[half+lane].used())
			if b := r.laneBudget(lane, half); used >= b {
				t.replaceLane(lane, fmt.Sprintf("its rooms took %.1f MB (budget %.1f MB)", mb(used), mb(b)))
			}
		}
	}
}

func mb(n uint64) float64 { return float64(n) / (1 << 20) }

// used is what this room carried since this side moved into it.
func (w *avitoWS) used() uint64 {
	total := w.stats.bytesSent.Load() + w.stats.bytesRecv.Load()
	if base := w.budgetBase.Load(); total > base {
		return total - base
	}
	return 0
}

// roomSpent: Avito refuses this side's data in room id.
func (t *AvitoTransport) roomSpent(id string) {
	r := &t.rot
	if !t.isClient {
		t.replaceLaneOf(id, "Avito refuses data in room "+short(id))
		return
	}
	r.mu.Lock()
	if r.told == nil {
		r.told = map[string]bool{}
	}
	told := r.told[id]
	r.told[id] = true
	r.mu.Unlock()
	if !told {
		utils.Infof("[AVITO] asking the exit for a new room instead of %s", id[:8])
		r.ctl.append("S " + id)
	}
}

func (t *AvitoTransport) onCtlText(text string) {
	gen, ids, spent := parseCtlLog(text)
	if gen > 0 {
		t.adoptRooms(gen, ids)
	}
	if !t.isClient {
		for _, id := range spent {
			t.replaceLaneOf(id, "the client cannot write into room "+short(id))
		}
	}
}

// parseCtlLog reads the log: the latest room list and its generation (0 if
// none), and the rooms reported spent.
func parseCtlLog(text string) (gen int, ids []string, spent []string) {
	best := ""
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		switch {
		case len(f) == 3 && f[0] == "R":
			g, err := strconv.Atoi(f[1])
			if err != nil || g <= 0 || g < gen || (g == gen && f[2] <= best) {
				continue
			}
			list, _, err := transport.UnpackRooms(f[2])
			if err != nil || len(list) == 0 {
				continue
			}
			gen, ids, best = g, list, f[2]
		case len(f) == 2 && f[0] == "S" && f[1] != "":
			spent = append(spent, f[1])
		}
	}
	return gen, ids, spent
}

// adoptRooms moves this side to the rooms of generation gen.
func (t *AvitoTransport) adoptRooms(gen int, ids []string) {
	r := &t.rot
	r.mu.Lock()
	if gen <= r.gen || len(ids) != len(r.ids) {
		r.mu.Unlock()
		return
	}
	type move struct {
		w        *avitoWS
		from, to string
	}
	var moves []move
	for p := range ids {
		if ids[p] == r.ids[p] {
			continue
		}
		var w *avitoWS
		if p < len(r.byPos) {
			w = r.byPos[p]
		}
		moves = append(moves, move{w, r.ids[p], ids[p]})
	}
	r.gen, r.ids = gen, append([]string(nil), ids...)
	r.mu.Unlock()
	for _, m := range moves {
		if m.w != nil {
			m.w.moveTo(m.to)
		}
		utils.Infof("[AVITO] room %s replaced by %s", short(m.from), short(m.to))
	}
	t.setRoomList(ids)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// replaceLaneOf gives the lane of room id new rooms, if id is in use.
func (t *AvitoTransport) replaceLaneOf(id, why string) {
	r := &t.rot
	r.mu.Lock()
	lane := -1
	for p, have := range r.ids {
		if have == id {
			lane = p % (len(r.ids) / 2)
			break
		}
	}
	r.mu.Unlock()
	if lane >= 0 {
		t.replaceLane(lane, why)
	}
}

// replaceLane (exit) makes lane two new rooms, writes the new list to the
// control room and moves over.
func (t *AvitoTransport) replaceLane(lane int, why string) {
	r := &t.rot
	r.mu.Lock()
	if r.busy == nil {
		r.busy, r.movedAt, r.pause = map[int]bool{}, map[int]time.Time{}, map[int]time.Duration{}
	}
	if r.busy[lane] {
		r.mu.Unlock()
		return
	}
	r.busy[lane] = true
	// What the lane's rooms took: little means they died of something else
	// than their data, and the next ones wait.
	var used uint64
	if half := len(r.byPos) / 2; lane < half {
		used = max(r.byPos[lane].used(), r.byPos[half+lane].used())
	}
	if used < r.quickBytes && !r.movedAt[lane].IsZero() {
		r.pause[lane] = min(max(2*r.pause[lane], lanePauseMin), lanePauseMax)
	} else {
		r.pause[lane] = 0
	}
	wait := time.Until(r.movedAt[lane].Add(r.pause[lane]))
	r.mu.Unlock()
	utils.Infof("[AVITO] lane %d: new rooms, %s", lane+1, why)
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.busy, lane)
			r.mu.Unlock()
		}()
		if wait > 0 {
			utils.Infof("[AVITO] lane %d: new rooms in %v (its last ones carried little)", lane+1, wait.Round(time.Second))
			if !sleepCtx(t.ctx, wait) {
				return
			}
		}
		r.create.Lock()
		defer r.create.Unlock()
		a, b, ok := t.newLaneRooms()
		if !ok {
			return
		}
		r.mu.Lock()
		ids := append([]string(nil), r.ids...)
		half := len(ids) / 2
		ids[lane], ids[half+lane] = a, b
		gen := r.gen + 1
		r.movedAt[lane] = time.Now()
		r.mu.Unlock()
		utils.Infof("[AVITO] lane %d: new rooms %s and %s (generation %d)", lane+1, a[:8], b[:8], gen)
		r.ctl.append(fmt.Sprintf("R %d %s", gen, t.packRooms(ids)))
		t.adoptRooms(gen, ids)
	}()
}

// newLaneRooms creates and joins two rooms, trying until the transport stops.
func (t *AvitoTransport) newLaneRooms() (string, string, bool) {
	var out [2]string
	for k := range out {
		for attempt := 0; ; attempt++ {
			id, err := createRoom(t.ctx, t.cookies)
			if err == nil {
				if jerr := registerRoom(t.ctx, id); jerr != nil && t.ctx.Err() == nil {
					utils.Debugf("[AVITO] join new room %s: %v (joined later)", id[:8], jerr)
				}
				out[k] = id
				break
			}
			if t.ctx.Err() != nil {
				return "", "", false
			}
			wait := min(t.config.RoomCreatePause<<min(attempt, 10), 5*time.Minute)
			utils.Debugf("[AVITO] new room attempt %d failed: %v (wait %v)", attempt+1, err, wait)
			if !sleepCtx(t.ctx, wait) {
				return "", "", false
			}
		}
	}
	return out[0], out[1], true
}

// ---- the rooms: moving to another one ----

// room is the room id, for other goroutines than run's.
func (w *avitoWS) room() string {
	w.idMu.Lock()
	defer w.idMu.Unlock()
	return w.roomID
}

// moveTo makes the room's connection go over to room id; its budget starts
// again.
func (w *avitoWS) moveTo(id string) {
	w.budgetBase.Store(w.stats.bytesSent.Load() + w.stats.bytesRecv.Load())
	w.next.Store(&id)
	w.writeMu.Lock()
	if w.conn != nil {
		w.conn.Close()
	}
	w.writeMu.Unlock()
}

// takeMove (run, between connections) switches to the room moveTo named.
func (w *avitoWS) takeMove() {
	if n := w.next.Swap(nil); n != nil {
		w.idMu.Lock()
		w.roomID = *n
		w.idMu.Unlock()
		w.needJoin = false
	}
}

// awaitMove (run, the room spent) reports it and waits for a new room; false
// when there will be none.
func (w *avitoWS) awaitMove() bool {
	if w.onSpent == nil {
		return false
	}
	w.onSpent(w.roomID)
	for w.next.Load() == nil {
		if !sleepCtx(w.ctx, time.Second) {
			return false
		}
	}
	return true
}

// ---- the control room ----

// ctlLog keeps the control room's document: it follows the text and appends
// lines to it.
type ctlLog struct {
	w      *avitoWS // for dial, subscribe and writeJSON
	onText func(text string)
	ready  chan struct{} // closed once the text was read
	once   sync.Once
	kick   chan struct{}

	mu    sync.Mutex
	queue []string
}

func newCtlLog(t *AvitoTransport, id string, onText func(string)) *ctlLog {
	return &ctlLog{
		w: &avitoWS{
			idx: -1, roomID: id, cookies: t.cookies, config: t.config, ctx: t.ctx, gate: t.gate,
			stats: &channelStats{idx: -1, roomID: id},
		},
		onText: onText,
		ready:  make(chan struct{}),
		kick:   make(chan struct{}, 1),
	}
}

// append adds a line to the log (in the background, again after a failure).
func (c *ctlLog) append(line string) {
	c.mu.Lock()
	c.queue = append(c.queue, line+"\n")
	c.mu.Unlock()
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *ctlLog) run() {
	w := c.w
	delay := time.Second
	for w.ctx.Err() == nil {
		up := time.Now()
		err := c.serve()
		if w.ctx.Err() != nil {
			return
		}
		if time.Since(up) > time.Minute {
			delay = time.Second
		}
		utils.Debugf("[AVITO] control room %s: %v", short(w.roomID), err)
		if w.needJoin {
			w.needJoin = false
			if jerr := registerRoom(w.ctx, w.roomID); jerr == nil {
				continue
			}
		}
		if !sleepCtx(w.ctx, delay) {
			return
		}
		delay = min(2*delay, 30*time.Second)
	}
}

func (c *ctlLog) serve() error {
	w := c.w
	conn, err := w.dial()
	if err != nil {
		return err
	}
	unhook := context.AfterFunc(w.ctx, func() { conn.Close() })
	defer unhook()
	w.writeMu.Lock()
	w.conn = conn
	w.writeMu.Unlock()
	defer func() {
		w.writeMu.Lock()
		w.conn = nil
		w.writeMu.Unlock()
		conn.Close()
	}()
	conn.SetReadLimit(w.config.MaxMessageBytes)
	if err := w.subscribe(); err != nil {
		return err
	}
	text := w.snap
	c.onText(text)
	c.once.Do(func() { close(c.ready) })

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(w.config.WSReadTimeout))
	})
	msgs := make(chan map[string]interface{}, 16)
	readErr := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			conn.SetReadDeadline(time.Now().Add(w.config.WSReadTimeout))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			var m map[string]interface{}
			if json.Unmarshal(raw, &m) != nil {
				continue
			}
			select {
			case msgs <- m:
			case <-done:
				return
			}
		}
	}()
	ping := time.NewTicker(avitoPingInterval)
	defer ping.Stop()

	var queued, sent string // the line in flight: as queued, and as written
	for {
		if sent == "" {
			c.mu.Lock()
			if len(c.queue) > 0 {
				queued = c.queue[0]
			}
			c.mu.Unlock()
			if queued != "" {
				sent = queued
				if text != "" && !strings.HasSuffix(text, "\n") {
					sent = "\n" + sent
				}
				var op []interface{}
				if n := jsLen(text); n > 0 {
					op = []interface{}{n, sent}
				} else {
					op = []interface{}{sent}
				}
				if err := w.writeJSON(map[string]interface{}{
					"a": "op", "c": "docs", "d": w.roomID,
					"v": w.ver, "src": w.connID, "seq": w.seq, "op": op,
				}); err != nil {
					return err
				}
			}
		}
		select {
		case <-w.ctx.Done():
			return errStopped
		case err := <-readErr:
			return err
		case <-c.kick:
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(w.config.WSWriteTimeout)); err != nil {
				return err
			}
		case m := <-msgs:
			if a, _ := m["a"].(string); a != "op" {
				continue
			}
			src, _ := m["src"].(string)
			if src != "" && src != w.connID { // a peer's op; ours (or the server's reply) comes back as the ack
				ops, _ := m["op"].([]interface{})
				nt, ok := applyText(text, ops)
				if !ok {
					return fmt.Errorf("an op that does not fit the log (%d characters): reading it again", jsLen(text))
				}
				text = nt
				if v, ok := m["v"].(float64); ok && int(v)+1 > w.ver {
					w.ver = int(v) + 1
				}
				c.onText(text)
				continue
			}
			if sent == "" {
				continue
			}
			if e := m["error"]; e != nil {
				return fmt.Errorf("append: %v", e)
			}
			text += sent
			if v, ok := m["v"].(float64); ok {
				w.ver = int(v) + 1
			} else {
				w.ver++
			}
			w.seq++
			c.mu.Lock()
			if len(c.queue) > 0 && c.queue[0] == queued {
				c.queue = c.queue[1:]
			}
			c.mu.Unlock()
			queued, sent = "", ""
			c.onText(text)
		}
	}
}

// jsLen is the length of s the way ShareDB's text type counts (UTF-16 units).
func jsLen(s string) int { return len(utf16.Encode([]rune(s))) }

// applyText applies a textv1 op (skip n, insert "s", delete {d: n}) to text.
func applyText(text string, ops []interface{}) (string, bool) {
	src := utf16.Encode([]rune(text))
	out := make([]uint16, 0, len(src))
	pos := 0
	for _, c := range ops {
		switch v := c.(type) {
		case float64:
			n := int(v)
			if n < 0 || pos+n > len(src) {
				return text, false
			}
			out = append(out, src[pos:pos+n]...)
			pos += n
		case string:
			out = append(out, utf16.Encode([]rune(v))...)
		case map[string]interface{}:
			n := -1
			switch d := v["d"].(type) {
			case float64:
				n = int(d)
			case string:
				n = jsLen(d)
			}
			if n < 0 || pos+n > len(src) {
				return text, false
			}
			pos += n
		default:
			return text, false
		}
	}
	out = append(out, src[pos:]...)
	return string(utf16.Decode(out)), true
}
