// Package pairdrop implements a reconnecting PairDrop file receiver.
package pairdrop

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pion/webrtc/v3"
)

// Config is immutable after New. Use one running client per IdentityPath.
type Config struct {
	SaveDir      string
	IdentityPath string
	Room         string
	ServerURL    string
	RetryDelay   time.Duration
}

type Device struct{ ID, Name, State string }
type Transfer struct {
	PeerID, Name, State, Path string
	Received, Size            int64
}

// Snapshot contains independent copies and is safe for GUI polling.
type Snapshot struct {
	Status, Name string
	Devices      []Device
	Transfers    []Transfer
	Logs         []string
}

func New(config Config) (*Node, error) {
	if config.Room != "" && !regexp.MustCompile("^[a-z]{5}$").MatchString(config.Room) {
		return nil, errors.New("公共房间必须是五个小写英文字母")
	}
	if config.SaveDir == "" {
		config.SaveDir = "."
	}
	var err error
	config.SaveDir, err = filepath.Abs(config.SaveDir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(config.SaveDir, 0755); err != nil {
		return nil, err
	}
	if config.IdentityPath == "" {
		dir, err := os.UserConfigDir()
		if err != nil {
			return nil, err
		}
		config.IdentityPath = filepath.Join(dir, "PairDropGo", identityFile)
	}
	config.IdentityPath, err = filepath.Abs(config.IdentityPath)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(config.IdentityPath), 0700); err != nil {
		return nil, err
	}
	if config.ServerURL == "" {
		config.ServerURL = pairDropURL
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 3 * time.Second
	}
	return &Node{config: config, room: config.Room, peers: make(map[string]*remotePeer),
		view: Snapshot{Status: "未连接"},
		rtc:  webrtc.Configuration{ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}}},
	}, nil
}

// Run blocks until ctx is cancelled. Network failures reconnect automatically.
// Concurrent Run calls on the same client are rejected.
func (n *Node) Run(ctx context.Context) error {
	if !n.running.CompareAndSwap(false, true) {
		return errors.New("客户端已经运行")
	}
	defer n.running.Store(false)
	defer n.setStatus("已停止", "")
	n.run(ctx, n.config.ServerURL, n.config.RetryDelay)
	return nil
}

// SendFiles sends the given local files to the peer identified by id.
// It returns immediately; the transfer runs in the background and progress can
// be observed through Snapshot. Only one batch per device may run at a time.
func (n *Node) SendFiles(id string, paths []string) error {
	if len(paths) == 0 {
		return errors.New("未选择任何文件")
	}
	n.mu.Lock()
	peer := n.peers[id]
	n.mu.Unlock()
	if peer == nil {
		return fmt.Errorf("设备 %s 不在线", id)
	}
	peer.mu.Lock()
	dc := peer.dc
	peer.mu.Unlock()
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errors.New("尚未与该设备建立可用的数据通道")
	}
	return peer.enqueue(paths)
}

func (n *Node) Snapshot() Snapshot {
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	s := n.view
	s.Devices = append([]Device(nil), s.Devices...)
	s.Transfers = append([]Transfer(nil), s.Transfers...)
	s.Logs = append([]string(nil), s.Logs...)
	return s
}

func (n *Node) logf(format string, args ...any) {
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	n.view.Logs = append(n.view.Logs, time.Now().Format("15:04:05")+"  "+fmt.Sprintf(format, args...))
	if len(n.view.Logs) > 150 {
		n.view.Logs = n.view.Logs[len(n.view.Logs)-150:]
	}
}
func (n *Node) setStatus(status, name string) {
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	n.view.Status = status
	if name != "" {
		n.view.Name = name
	}
}
func (n *Node) device(id, name, state string) {
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	for i := range n.view.Devices {
		if n.view.Devices[i].ID == id {
			if name != "" {
				n.view.Devices[i].Name = name
			}
			n.view.Devices[i].State = state
			return
		}
	}
	if state == "closed" || state == "离线" {
		return
	}
	if len(n.view.Devices) >= 100 {
		n.view.Devices = n.view.Devices[1:]
	}
	n.view.Devices = append(n.view.Devices, Device{id, name, state})
}

// renameDevice updates only the display name of an already known device,
// keeping its state (e.g. "可收发文件") intact. Device names are stored as
// "显示名 / 设备名", so only the leading display name is replaced.
func (n *Node) renameDevice(id, displayName string) {
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	for i := range n.view.Devices {
		if n.view.Devices[i].ID != id {
			continue
		}
		if _, deviceName, ok := strings.Cut(n.view.Devices[i].Name, " / "); ok {
			n.view.Devices[i].Name = displayName + " / " + deviceName
		} else {
			n.view.Devices[i].Name = displayName
		}
		return
	}
}
func (n *Node) transfer(id, name string, received, size int64, state, path string) {
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	item := Transfer{id, name, state, path, received, size}
	for i := len(n.view.Transfers) - 1; i >= 0; i-- {
		t := n.view.Transfers[i]
		if t.PeerID == id && t.Name == name && (t.State == "接收中" || t.State == "发送中") {
			n.view.Transfers[i] = item
			return
		}
	}
	if len(n.view.Transfers) >= 100 {
		n.view.Transfers = n.view.Transfers[1:]
	}
	n.view.Transfers = append(n.view.Transfers, item)
}
