package avito

// Several rooms on one connection (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/avito_mux.py).
//
// Every room had a ShareDB connection of its own, and Avito takes about 10 new
// connections a minute from an address: a phone needed ~2.5 minutes to bring
// its 16 rooms up, and a lane that moved to new rooms waited for a connection
// too. One ShareDB connection can subscribe to and write into several
// documents (checked 2026-11-05: 4 rooms on one connection, all ops acked and
// relayed, 4.7 MB/s against 1.0 for one room), so the rooms now share
// connections, muxRooms per connection (AVITO_MUX_ROOMS, 1 = one each): 16
// rooms take 4. A room on a shared connection is a muxRoom, which the room's
// code uses like its own WebSocket: messages for its document come to it,
// what it writes goes out on the shared socket.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

// wsConn is what a room needs of its connection: a WebSocket of its own or a
// muxRoom on a shared one.
type wsConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetReadLimit(limit int64)
	SetPongHandler(h func(appData string) error)
	Close() error
}

var errMuxGone = errors.New("avito: the shared connection went down")

// muxRooms is how many rooms share a connection (AVITO_MUX_ROOMS; 1: none).
func muxRooms() int {
	if n, err := strconv.Atoi(os.Getenv("AVITO_MUX_ROOMS")); err == nil && n >= 1 {
		return n
	}
	return 4
}

// muxConn is one shared ShareDB connection.
type muxConn struct {
	idx     int
	ctx     context.Context
	config  AvitoConfig
	gate    *dialGate
	cookies func() string

	dialMu  sync.Mutex // one dial at a time
	writeMu sync.Mutex // one writer on the socket

	mu    sync.Mutex
	ws    *websocket.Conn
	id    string // ShareDB's id for the connection: the src of our ops
	rooms map[string]*muxRoom

	seqBase atomic.Int64
}

// wireMux puts the rooms on shared connections: a lane's two rooms on the
// same one, muxRooms() rooms to a connection.
func (t *AvitoTransport) wireMux() {
	per := muxRooms()
	if per <= 1 {
		return
	}
	lanes := max(len(t.txWss), len(t.rxWss))
	pairs := max(per/2, 1)
	var conns []*muxConn
	get := func(k int) *muxConn {
		for len(conns) <= k {
			conns = append(conns, &muxConn{
				idx: len(conns), ctx: t.ctx, config: t.config, gate: t.gate,
				cookies: func() string { return t.cookies },
				rooms:   map[string]*muxRoom{},
			})
		}
		return conns[k]
	}
	for i := 0; i < lanes; i++ {
		m := get(i / pairs)
		if i < len(t.txWss) {
			t.txWss[i].mux = m
		}
		if i < len(t.rxWss) {
			t.rxWss[i].mux = m
		}
	}
	utils.Debugf("[AVITO] %d rooms on %d shared connections", len(t.txWss)+len(t.rxWss), len(conns))
}

// seqStart is where a room's op numbers start on its connection: rooms that
// share a connection share its src, so each attach gets numbers of its own.
func (w *avitoWS) seqStart() int {
	if w.mux == nil {
		return 1
	}
	return int(w.mux.seqBase.Add(1<<24)) + 1
}

// attach gives room w its view of the shared connection, dialing it first if
// it is down.
func (m *muxConn) attach(w *avitoWS) (wsConn, error) {
	sock, id, err := m.socket()
	if err != nil {
		return nil, err
	}
	init, _ := json.Marshal(map[string]string{"a": "init", "id": id})
	r := &muxRoom{m: m, sock: sock, room: w.roomID, first: init,
		in: make(chan []byte, 256), closed: make(chan struct{})}
	m.mu.Lock()
	if m.ws != sock {
		m.mu.Unlock()
		return nil, errMuxGone
	}
	old := m.rooms[w.roomID]
	m.rooms[w.roomID] = r
	m.mu.Unlock()
	if old != nil {
		old.shut()
	}
	return r, nil
}

func (m *muxConn) socket() (*websocket.Conn, string, error) {
	m.dialMu.Lock()
	defer m.dialMu.Unlock()
	m.mu.Lock()
	if m.ws != nil {
		s, id := m.ws, m.id
		m.mu.Unlock()
		return s, id, nil
	}
	m.mu.Unlock()

	d := &avitoWS{cookies: m.cookies(), config: m.config, ctx: m.ctx, gate: m.gate}
	c, err := d.dial()
	if err != nil {
		return nil, "", err
	}
	conn := c.(*websocket.Conn)
	conn.SetReadLimit(m.config.MaxMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(m.config.WSReadTimeout))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return nil, "", err
	}
	var init struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &init) != nil || init.ID == "" {
		conn.Close()
		return nil, "", errors.New("avito: no connection id from Avito")
	}
	m.mu.Lock()
	m.ws, m.id = conn, init.ID
	m.mu.Unlock()
	context.AfterFunc(m.ctx, func() { conn.Close() })
	go m.readLoop(conn)
	go m.pingLoop(conn)
	utils.Debugf("[AVITO] shared connection %d up", m.idx)
	return conn, init.ID, nil
}

// readLoop hands every message to the room of its document.
func (m *muxConn) readLoop(conn *websocket.Conn) {
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(m.config.WSReadTimeout))
	})
	for {
		_ = conn.SetReadDeadline(time.Now().Add(m.config.WSReadTimeout))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			m.drop(conn, err)
			return
		}
		doc, unsub := docOf(raw)
		if doc == "" || unsub {
			continue
		}
		m.mu.Lock()
		r := m.rooms[doc]
		m.mu.Unlock()
		if r == nil {
			continue // a room that left; what it still got is lost, the stripe resends
		}
		select {
		case r.in <- raw:
		case <-r.closed:
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *muxConn) pingLoop(conn *websocket.Conn) {
	t := time.NewTicker(avitoPingInterval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		m.mu.Lock()
		live := m.ws == conn
		m.mu.Unlock()
		if !live {
			return
		}
		// WriteControl may run alongside the other writes (gorilla/websocket).
		if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(m.config.WSWriteTimeout)); err != nil {
			m.drop(conn, err)
			return
		}
	}
}

// drop ends the socket and every room on it; the rooms dial again.
func (m *muxConn) drop(conn *websocket.Conn, err error) {
	m.mu.Lock()
	var rooms []*muxRoom
	if m.ws == conn {
		m.ws, m.id = nil, ""
		for _, r := range m.rooms {
			rooms = append(rooms, r)
		}
		m.rooms = map[string]*muxRoom{}
		m.seqBase.Store(0)
	}
	m.mu.Unlock()
	conn.Close()
	if len(rooms) > 0 && m.ctx.Err() == nil {
		utils.Debugf("[AVITO] shared connection %d down (%d rooms): %v", m.idx, len(rooms), err)
	}
	for _, r := range rooms {
		r.shut()
	}
}

// docOf finds the document a message is about, and whether it answers an
// unsubscribe, without decoding an op's data (up to 350 KB).
func docOf(raw []byte) (string, bool) {
	head := raw
	if len(head) > 512 {
		head = head[:512]
	}
	if i := bytes.Index(head, []byte(`"d":"`)); i >= 0 {
		rest := head[i+5:]
		if j := bytes.IndexByte(rest, '"'); j >= 0 {
			return string(rest[:j]), bytes.Contains(head, []byte(`"a":"us"`))
		}
	}
	var m struct {
		A string `json:"a"`
		D string `json:"d"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return "", false
	}
	return m.D, m.A == "us"
}

// muxRoom is one room's view of a shared connection.
type muxRoom struct {
	m      *muxConn
	sock   *websocket.Conn
	room   string
	first  []byte // the init message the room expects first
	in     chan []byte
	closed chan struct{}
	once   sync.Once
}

func (r *muxRoom) ReadMessage() (int, []byte, error) {
	if b := r.first; b != nil {
		r.first = nil
		return websocket.TextMessage, b, nil
	}
	select {
	case b := <-r.in:
		return websocket.TextMessage, b, nil
	case <-r.closed:
		return 0, nil, errMuxGone
	}
}

func (r *muxRoom) WriteMessage(mt int, data []byte) error {
	select {
	case <-r.closed:
		return errMuxGone
	default:
	}
	r.m.writeMu.Lock()
	_ = r.sock.SetWriteDeadline(time.Now().Add(r.m.config.WSWriteTimeout))
	err := r.sock.WriteMessage(mt, data)
	r.m.writeMu.Unlock()
	if err != nil {
		r.m.drop(r.sock, err)
	}
	return err
}

// The shared socket keeps itself alive (pingLoop, its read deadline): a room
// on it waits for its messages as long as the socket is up.
func (r *muxRoom) WriteControl(int, []byte, time.Time) error { return nil }
func (r *muxRoom) SetReadDeadline(time.Time) error           { return nil }
func (r *muxRoom) SetWriteDeadline(time.Time) error          { return nil }
func (r *muxRoom) SetReadLimit(int64)                        {}
func (r *muxRoom) SetPongHandler(func(string) error)         {}

// Close takes the room off the connection; the socket stays for the others.
func (r *muxRoom) Close() error {
	r.shut()
	m := r.m
	m.mu.Lock()
	mine := m.rooms[r.room] == r
	if mine {
		delete(m.rooms, r.room)
	}
	live := m.ws == r.sock
	m.mu.Unlock()
	if mine && live {
		// Before the room can subscribe again: Avito handles a connection's
		// messages in order.
		msg, _ := json.Marshal(map[string]interface{}{"a": "us", "c": "docs", "d": r.room})
		m.writeMu.Lock()
		_ = r.sock.SetWriteDeadline(time.Now().Add(m.config.WSWriteTimeout))
		_ = r.sock.WriteMessage(websocket.TextMessage, msg)
		m.writeMu.Unlock()
	}
	return nil
}

func (r *muxRoom) shut() { r.once.Do(func() { close(r.closed) }) }
