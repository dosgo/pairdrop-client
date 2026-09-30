package pairdrop

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
)

func TestDataChannelTransfer(t *testing.T) {
	dir := t.TempDir()
	client, err := New(Config{SaveDir: dir, IdentityPath: filepath.Join(t.TempDir(), "identity.json")})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	receiver, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	peer := &remotePeer{owner: client, id: "test", pc: receiver}
	receiver.OnDataChannel(func(dc *webrtc.DataChannel) { client.bindDataChannel(peer, dc) })
	dc, err := sender.CreateDataChannel("data-channel", nil)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	dc.OnOpen(func() { close(opened) })
	messages := make(chan map[string]any, 256)
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		var msg map[string]any
		if json.Unmarshal(m.Data, &msg) == nil {
			messages <- msg
		}
	})
	offer, err := sender.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathering := webrtc.GatheringCompletePromise(sender)
	if err = sender.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gathering
	if err = receiver.SetRemoteDescription(*sender.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	answer, err := receiver.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathering = webrtc.GatheringCompletePromise(receiver)
	if err = receiver.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	<-gathering
	if err = sender.SetRemoteDescription(*receiver.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("channel did not open")
	}
	send := func(v any) {
		t.Helper()
		data, _ := json.Marshal(v)
		if err := dc.SendText(string(data)); err != nil {
			t.Fatal(err)
		}
	}
	lastProgress := float64(0)
	wait := func(kind string) map[string]any {
		t.Helper()
		timeout := time.NewTimer(10 * time.Second)
		defer timeout.Stop()
		for {
			select {
			case msg := <-messages:
				if msg["type"] == "progress" {
					p := msg["progress"].(float64)
					if p < lastProgress {
						t.Fatal("progress regressed")
					}
					lastProgress = p
				}
				if msg["type"] == kind {
					return msg
				}
			case <-timeout.C:
				t.Fatalf("waiting for %s; logs: %v", kind, client.Snapshot().Logs)
				return nil
			}
		}
	}
	payload := bytes.Repeat([]byte("PairDrop test!"), 100000)
	send(map[string]any{"type": "request", "header": []FileMeta{{Name: "test.bin", Size: int64(len(payload))}, {Name: "empty.txt", Size: 0}}, "totalSize": len(payload)})
	if wait("files-transfer-response")["accepted"] != true {
		t.Fatal("request rejected")
	}
	send(map[string]any{"type": "header", "name": "test.bin", "size": len(payload)})
	for offset := 0; offset < len(payload); {
		end := offset + 64000
		if end > len(payload) {
			end = len(payload)
		}
		if err := dc.Send(payload[offset:end]); err != nil {
			t.Fatal(err)
		}
		offset = end
		if offset == 1024000 {
			send(map[string]any{"type": "partition", "offset": offset})
			if wait("partition-received")["offset"] != float64(offset) {
				t.Fatal("wrong partition offset")
			}
		}
	}
	wait("file-transfer-complete")
	send(map[string]any{"type": "header", "name": "empty.txt", "size": 0})
	wait("file-transfer-complete")
	if lastProgress != 1 {
		t.Fatalf("final progress=%v", lastProgress)
	}
	actual, err := os.ReadFile(filepath.Join(dir, "test.bin"))
	if err != nil || !bytes.Equal(actual, payload) {
		t.Fatal("file content mismatch", err)
	}
	info, err := os.Stat(filepath.Join(dir, "empty.txt"))
	if err != nil || info.Size() != 0 {
		t.Fatal("empty file missing", err)
	}
	if len(client.Snapshot().Transfers) != 2 {
		t.Fatal("incorrect transfer history")
	}
}

func TestAvailablePathAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"../escape", "..", "file:stream", "a/b", "a\\b"} {
		if _, err := availablePath(dir, name); err == nil {
			t.Fatalf("accepted invalid path %q", name)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	path, err := availablePath(dir, "file.txt")
	if err != nil || filepath.Base(path) != "file (1).txt" {
		t.Fatal(path, err)
	}
	n, err := New(Config{SaveDir: dir, IdentityPath: filepath.Join(dir, "identity.json")})
	if err != nil {
		t.Fatal(err)
	}
	n.device("a", "device", "ready")
	snapshot := n.Snapshot()
	snapshot.Devices[0].Name = "changed"
	if n.Snapshot().Devices[0].Name != "device" {
		t.Fatal("snapshot shares storage")
	}
}
