package pairdrop

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestReconnectAndPeerDeparture(t *testing.T) {
	var attempts atomic.Int32
	verified := make(chan struct{}, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if attempts.Add(1) == 1 {
			return
		} // Unexpected EOF must trigger reconnect.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteJSON(map[string]any{"type": "peer-left", "peerId": "gone"}); err != nil {
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": "ping"}); err != nil {
			return
		}
		var reply struct {
			Type string `json:"type"`
		}
		if conn.ReadJSON(&reply) == nil && reply.Type == "pong" {
			verified <- struct{}{}
		}
		_, _, _ = conn.ReadMessage() // Cancellation should unblock the client and server.
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := &Node{peers: make(map[string]*remotePeer)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		node.run(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), 10*time.Millisecond)
	}()
	select {
	case <-verified:
	case <-time.After(5 * time.Second):
		t.Fatal("did not reconnect and respond to ping after peer departure")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	if attempts.Load() != 2 {
		t.Fatalf("connections = %d, want 2", attempts.Load())
	}
}
