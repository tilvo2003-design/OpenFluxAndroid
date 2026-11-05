package avito

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

// countingSocketIO answers joins the way Avito's Socket.IO does, and counts
// connections and joins.
func countingSocketIO(t *testing.T) (conns, joins *atomic.Int32) {
	conns, joins = new(atomic.Int32), new(atomic.Int32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/socket.io/") {
			http.NotFound(w, r)
			return
		}
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		conns.Add(1)
		ws.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"x","pingInterval":25000}`))
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			switch s := string(raw); {
			case s == "40":
				ws.WriteMessage(websocket.TextMessage, []byte(`40{"sid":"y"}`))
			case strings.HasPrefix(s, `42["join"`):
				joins.Add(1)
				ws.WriteMessage(websocket.TextMessage, []byte(`2`))
				ws.WriteMessage(websocket.TextMessage, []byte(`42["initialize",{}]`))
			}
		}
	}))
	old := baseURL
	baseURL = srv.URL
	t.Cleanup(func() { baseURL = old; srv.Close() })
	return conns, joins
}

func TestRoomsAreJoinedOverOneConnection(t *testing.T) {
	conns, joins := countingSocketIO(t)
	n, err := registerRooms(context.Background(), []string{"room-a", "room-b", "room-c"})
	if err != nil || n != 3 {
		t.Fatalf("joined %d: %v", n, err)
	}
	if conns.Load() != 1 || joins.Load() != 3 {
		t.Fatalf("%d connections for %d joins, want 1 for 3", conns.Load(), joins.Load())
	}
}
