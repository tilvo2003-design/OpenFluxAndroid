package avito

// Several rooms joined over one connection (patched in by
// tilvo2003-design/OpenFluxAndroid, .github/patches/avito_join_batch.py).
//
// A new room must be joined once (registerRoom) before its document can be
// made, and every join opened a Socket.IO connection of its own: one of the
// ~10 connections a minute Avito allows an address, so 16 new rooms took
// three minutes and a lane waited ~25 s for its new pair. One connection can
// join several rooms (checked 2026-11-06: three rooms joined over one, all
// three documents made after), so new rooms are joined together.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/netbind"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// registerRooms joins ids over one Socket.IO connection, in order; it returns
// how many it joined.
func registerRooms(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if !processGate.wait(ctx) {
		return 0, errStopped
	}
	wsURL := strings.Replace(baseURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1) + "/socket.io/?EIO=4&transport=websocket"
	header := http.Header{}
	header.Set("User-Agent", avitoUA)
	header.Set("Origin", baseURL)
	dialer := websocket.Dialer{
		NetDialContext:    netbind.DialContext,
		HandshakeTimeout:  15 * time.Second,
		EnableCompression: true,
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable) {
			processGate.limited(parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()))
			return 0, fmt.Errorf("join: HTTP %d (too many connections from this address): %w", resp.StatusCode, err)
		}
		return 0, fmt.Errorf("join: %w", err)
	}
	processGate.ok()
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	read := func(prefix string) (string, error) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(deadline)
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return "", err
			}
			s := string(raw)
			if s == "2" {
				_ = conn.WriteMessage(websocket.TextMessage, []byte("3"))
				continue
			}
			if strings.HasPrefix(s, prefix) {
				return s, nil
			}
			if strings.HasPrefix(s, `42["join-error"`) {
				return "", fmt.Errorf("join refused: %s", truncate(raw, 120))
			}
		}
		return "", fmt.Errorf("no %q from Avito in 10 s", prefix)
	}
	if _, err := read("0{"); err != nil {
		return 0, fmt.Errorf("join: engine.io open: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		return 0, fmt.Errorf("join: %w", err)
	}
	if _, err := read("40"); err != nil {
		return 0, fmt.Errorf("join: socket.io connect: %w", err)
	}
	for i, id := range ids {
		payload, _ := json.Marshal([]interface{}{"join", map[string]string{"name": joinName, "roomId": id}})
		if err := conn.WriteMessage(websocket.TextMessage, append([]byte("42"), payload...)); err != nil {
			return i, fmt.Errorf("join %s: %w", short(id), err)
		}
		if _, err := read(`42["initialize"`); err != nil {
			return i, fmt.Errorf("join %s: %w", short(id), err)
		}
	}
	return len(ids), nil
}

// joinRooms joins new rooms together; the ones it could not join are joined
// when their document is first made (needJoin).
func (t *AvitoTransport) joinRooms(ids []string) {
	n, err := registerRooms(t.ctx, ids)
	if err != nil && t.ctx.Err() == nil {
		utils.Debugf("[AVITO] joined %d of %d new rooms: %v (the rest when first used)", n, len(ids), err)
		return
	}
	utils.Debugf("[AVITO] joined %d new rooms over one connection", n)
}
