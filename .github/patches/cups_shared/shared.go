package cupsonline

// One session for both ends (patched in by tilvo2003-design/OpenFluxAndroid,
// .github/patches/cups_shared_session.py).
//
// cups.online now lets only the session that created a room into it (seen
// 2026-11-04: the creator gets the room page, anyone else a 403 "Отсутствуют
// права доступа", public room or not). So the exit creates its rooms in one
// session and the room list carries that session ("sess" in its JSON, older
// builds ignore it): the client, and the exit after a restart, enter the rooms
// as that session. Both ends are then the same user, and Centrifugo hands
// every cursor update to all of the room's connections, the sender's too: an
// update from our own user is the peer's unless it is one we just sent
// ourselves, which each end tells by its content.

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

// sharedOn: CUPS_SHARED=0 creates rooms the old way, one session each, with
// no session in the list.
func sharedOn() bool { return os.Getenv("CUPS_SHARED") != "0" }

var errSameRoom = errors.New("the session gave back a room it had made already")

// cupsListV2 is the striped room list with the rooms' sessions: one for all
// rooms, or one per room.
type cupsListV2 struct {
	V     int      `json:"v"`
	Rooms []string `json:"rooms"`
	Sess  []string `json:"sess,omitempty"`
}

// packList is transport.PackRooms with the sessions, when there are any.
func packList(ids []string, striped bool, owners []string) string {
	sess := compactOwners(owners)
	if !striped || len(sess) == 0 {
		return transport.PackRooms(ids, striped)
	}
	raw, _ := json.Marshal(cupsListV2{V: 2, Rooms: ids, Sess: sess})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func compactOwners(owners []string) []string {
	if len(owners) == 0 {
		return nil
	}
	same := true
	for _, o := range owners {
		if o == "" {
			return nil // a room without its session: the list cannot carry them
		}
		same = same && o == owners[0]
	}
	if same {
		return owners[:1]
	}
	return owners
}

// parseOwners returns each of the n rooms' session from --url's list, nil
// when it has none.
func parseOwners(rawURL string, n int) []string {
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
		var v cupsListV2
		if json.Unmarshal(raw, &v) != nil || len(v.Sess) == 0 {
			continue
		}
		switch len(v.Sess) {
		case 1:
			out := make([]string, n)
			for i := range out {
				out[i] = v.Sess[0]
			}
			return out
		case n:
			return v.Sess
		}
	}
	return nil
}

// newSessionClient is the HTTP client authorize makes, with the cookies of
// session (as "name=value; ...") already in its jar; nil for "".
func newSessionClient(session string) *http.Client {
	if session == "" {
		return nil
	}
	jar, _ := cookiejar.New(nil)
	u, err := url.Parse(baseRoomURL)
	if err == nil {
		var cs []*http.Cookie
		for _, part := range strings.Split(session, ";") {
			if k, v, ok := strings.Cut(strings.TrimSpace(part), "="); ok && k != "" {
				cs = append(cs, &http.Cookie{Name: k, Value: v, Path: "/"})
			}
		}
		jar.SetCookies(u, cs)
	}
	return &http.Client{
		Jar:     jar,
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
}

// sessionOf is the session in client's jar, without the CSRF token (the room
// page hands out a new one).
func sessionOf(client *http.Client) string {
	if client == nil || client.Jar == nil {
		return ""
	}
	u, err := url.Parse(baseRoomURL)
	if err != nil {
		return ""
	}
	var parts []string
	for _, c := range client.Jar.Cookies(u) {
		if c.Name != "csrftoken" {
			parts = append(parts, c.Name+"="+c.Value)
		}
	}
	return strings.Join(parts, "; ")
}

func ownersOf(auths []*cupsAuth) []string {
	if !sharedOn() {
		return nil
	}
	out := make([]string, len(auths))
	for i, a := range auths {
		out[i] = sessionOf(a.httpClient)
	}
	return out
}

// roomMaker keeps the rooms an exit creates in one session; when that session
// hands back a room it made already, the next room starts a fresh one (the
// list then carries a session per room).
type roomMaker struct {
	client *http.Client
	seen   map[string]bool
}

func newRoomMaker() *roomMaker {
	if !sharedOn() {
		return nil
	}
	return &roomMaker{seen: map[string]bool{}}
}

func (m *roomMaker) session() *http.Client {
	if m == nil {
		return nil
	}
	return m.client
}

func (m *roomMaker) made(a *cupsAuth) bool {
	if m == nil {
		return true
	}
	if m.seen[a.roomUUID] {
		m.client = nil
		return false
	}
	m.seen[a.roomUUID] = true
	m.client = a.httpClient
	return true
}

// ownerClient is a client in room i's session, nil when the list has none.
func (t *CupsonlineTransport) ownerClient(i int) *http.Client {
	if i < len(t.owners) {
		return newSessionClient(t.owners[i])
	}
	return nil
}

func (t *CupsonlineTransport) ownerOf(i int) string {
	if i < len(t.owners) {
		return t.owners[i]
	}
	return ""
}

func (t *CupsonlineTransport) packIDs(ids []string) string {
	return packList(ids, t.striped, t.owners)
}

// ---- telling our own updates from the peer's ----

// sentSet remembers the updates a room sent lately, by content.
type sentSet struct {
	mu   sync.Mutex
	at   map[uint64]time.Time
	adds int
}

const echoWindow = 30 * time.Second

func payloadHash(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

func (s *sentSet) note(b []byte) {
	now := time.Now()
	h := payloadHash(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.at == nil {
		s.at = map[uint64]time.Time{}
	}
	s.at[h] = now
	if s.adds++; s.adds%1024 == 0 {
		for k, t := range s.at {
			if now.Sub(t) > echoWindow {
				delete(s.at, k)
			}
		}
	}
}

// echo reports (and forgets) whether b is an update this room sent.
func (s *sentSet) echo(b []byte) bool {
	h := payloadHash(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.at[h]; ok {
		delete(s.at, h)
		return true
	}
	return false
}
