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

func TestSendFilesToPeer(t *testing.T) {
	dir := t.TempDir()
	node, err := New(Config{SaveDir: dir, IdentityPath: filepath.Join(t.TempDir(), "identity.json")})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("PairDrop send test!"), 100000)
	binPath := filepath.Join(dir, "send.bin")
	if err := os.WriteFile(binPath, payload, 0600); err != nil {
		t.Fatal(err)
	}
	emptyPath := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(emptyPath, nil, 0600); err != nil {
		t.Fatal(err)
	}

	senderPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer senderPC.Close()
	remotePC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer remotePC.Close()

	peer := newRemotePeer(node, "remote", "", "", senderPC)
	go peer.sendLoop()
	defer peer.stopSend()

	type received struct {
		meta FileMeta
		data []byte
	}
	completed := make(chan received, 4)
	// remotePC 模拟网页端接收方：按 PairDrop 协议回应 response/partition/complete。
	remotePC.OnDataChannel(func(dc *webrtc.DataChannel) {
		var pendingMeta []FileMeta
		var idx int
		var buf []byte
		var expected int64
		dc.OnMessage(func(m webrtc.DataChannelMessage) {
			if !m.IsString {
				buf = append(buf, m.Data...)
				if expected > 0 && int64(len(buf)) == expected {
					completed <- received{meta: pendingMeta[idx], data: append([]byte(nil), buf...)}
					idx++
					buf = nil
					_ = dc.SendText(`{"type":"file-transfer-complete"}`)
				}
				return
			}
			var msg DataMessage
			if err := json.Unmarshal(m.Data, &msg); err != nil {
				t.Errorf("invalid message: %v", err)
				return
			}
			switch msg.Type {
			case "request":
				pendingMeta = append([]FileMeta(nil), msg.Header...)
				idx, buf, expected = 0, nil, 0
				_ = dc.SendText(`{"type":"files-transfer-response","accepted":true}`)
			case "header":
				buf, expected = nil, msg.Size
				if msg.Size == 0 {
					completed <- received{meta: pendingMeta[idx]}
					idx++
					_ = dc.SendText(`{"type":"file-transfer-complete"}`)
				}
			case "partition":
				data, _ := json.Marshal(map[string]any{"type": "partition-received", "offset": msg.Offset})
				_ = dc.SendText(string(data))
			}
		})
	})

	dc, err := senderPC.CreateDataChannel("data-channel", nil)
	if err != nil {
		t.Fatal(err)
	}
	node.bindDataChannel(peer, dc)
	node.mu.Lock()
	node.peers["remote"] = peer
	node.mu.Unlock()

	opened := make(chan struct{})
	dc.OnOpen(func() { close(opened) })
	offer, err := senderPC.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathering := webrtc.GatheringCompletePromise(senderPC)
	if err = senderPC.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gathering
	if err = remotePC.SetRemoteDescription(*senderPC.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	answer, err := remotePC.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathering = webrtc.GatheringCompletePromise(remotePC)
	if err = remotePC.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	<-gathering
	if err = senderPC.SetRemoteDescription(*remotePC.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("channel did not open")
	}

	if err = node.SendFiles("remote", []string{binPath, emptyPath}); err != nil {
		t.Fatal(err)
	}

	got := make([]received, 0, 2)
	timeout := time.NewTimer(20 * time.Second)
	defer timeout.Stop()
	for len(got) < 2 {
		select {
		case r := <-completed:
			got = append(got, r)
		case <-timeout.C:
			t.Fatalf("timeout waiting for files; logs: %v", node.Snapshot().Logs)
		}
	}
	if got[0].meta.Name != "send.bin" || !bytes.Equal(got[0].data, payload) {
		t.Fatalf("send.bin mismatch: %+v", got[0].meta)
	}
	if got[1].meta.Name != "empty.txt" || len(got[1].data) != 0 {
		t.Fatalf("empty.txt mismatch: %+v", got[1].meta)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		transfers := node.Snapshot().Transfers
		if len(transfers) == 2 && transfers[0].State == "已发送" && transfers[1].State == "已发送" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sender transfer records not complete: %v", transfers)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDisplayNameChanged(t *testing.T) {
	dir := t.TempDir()
	node, err := New(Config{SaveDir: dir, IdentityPath: filepath.Join(dir, "identity.json")})
	if err != nil {
		t.Fatal(err)
	}
	node.device("peer-1", "Turquoise Whale / Windows Chrome", "可收发文件")
	peer := newRemotePeer(node, "peer-1", "", "", nil)

	// 网页端改名后只通过数据通道广播 display-name-changed。
	payload, _ := json.Marshal(map[string]any{"type": "display-name-changed", "displayName": "十三pc"})
	if err := peer.handleDataMessage(webrtc.DataChannelMessage{IsString: true, Data: payload}); err != nil {
		t.Fatal(err)
	}
	device := node.Snapshot().Devices[0]
	if device.Name != "十三pc / Windows Chrome" {
		t.Fatalf("display name not updated: %q", device.Name)
	}
	if device.State != "可收发文件" {
		t.Fatalf("state must be preserved: %q", device.State)
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
