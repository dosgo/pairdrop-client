package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

const pairDropURL = "wss://pairdrop.net/server?webrtc_supported=true"
const identityFile = ".pairdrop-identity.json"

type identity struct {
	PeerID     string `json:"peerId"`
	PeerIDHash string `json:"peerIdHash"`
}

type PeerInfo struct {
	ID           string `json:"id"`
	RTCSupported bool   `json:"rtcSupported"`
	Name         struct {
		DisplayName string `json:"displayName"`
		DeviceName  string `json:"deviceName"`
	} `json:"name"`
}

type SignalMessage struct {
	Type        string                     `json:"type"`
	PeerID      string                     `json:"peerId,omitempty"`
	PeerIDHash  string                     `json:"peerIdHash,omitempty"`
	DisplayName string                     `json:"displayName,omitempty"`
	DeviceName  string                     `json:"deviceName,omitempty"`
	Peer        *PeerInfo                  `json:"peer,omitempty"`
	Peers       []PeerInfo                 `json:"peers,omitempty"`
	Sender      *PeerInfo                  `json:"sender,omitempty"`
	RoomType    string                     `json:"roomType,omitempty"`
	RoomID      string                     `json:"roomId,omitempty"`
	SDP         *webrtc.SessionDescription `json:"sdp,omitempty"`
	ICE         *webrtc.ICECandidateInit   `json:"ice,omitempty"`
	PublicRoom  string                     `json:"publicRoomId,omitempty"`
}

type FileMeta struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mime string `json:"mime"`
}

type DataMessage struct {
	Type      string     `json:"type"`
	Header    []FileMeta `json:"header,omitempty"`
	TotalSize int64      `json:"totalSize,omitempty"`
	Name      string     `json:"name,omitempty"`
	Size      int64      `json:"size,omitempty"`
	Mime      string     `json:"mime,omitempty"`
	Offset    int64      `json:"offset,omitempty"`
	Text      string     `json:"text,omitempty"`
}

type incomingFile struct {
	meta      FileMeta
	received  int64
	file      *os.File
	tempPath  string
	finalPath string
}

type remotePeer struct {
	id       string
	roomType string
	roomID   string
	pc       *webrtc.PeerConnection
	dc       *webrtc.DataChannel

	mu             sync.Mutex
	remoteSet      bool
	pendingICE     []webrtc.ICECandidateInit
	requestedFiles []FileMeta
	currentFile    *incomingFile
	totalSize      int64
	completedBytes int64
	lastProgress   float64
}

type Node struct {
	wsConn *websocket.Conn
	wsMu   sync.Mutex
	peers  map[string]*remotePeer
	mu     sync.Mutex
	room   string
}

func main() {
	room := flag.String("room", "", "PairDrop 五字符公共房间；留空则加入同公网 IP 房间")
	flag.Parse()

	node := &Node{peers: make(map[string]*remotePeer), room: strings.ToLower(*room)}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	node.run(ctx, pairDropURL, 3*time.Second)
}

func (node *Node) run(ctx context.Context, endpoint string, retryDelay time.Duration) {
	for ctx.Err() == nil {
		err := node.connect(ctx, endpoint)
		node.close()
		if ctx.Err() != nil {
			return
		}
		log.Printf("信令连接中断: %v；%s 后自动重连", err, retryDelay)
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (node *Node) connect(ctx context.Context, endpoint string) error {
	header := http.Header{}
	header.Set("User-Agent", "PairDrop-Go-Receiver/1.0")

	wsURL := endpoint
	if saved, err := loadIdentity(); err == nil && saved.PeerID != "" && saved.PeerIDHash != "" {
		u, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		q := u.Query()
		q.Set("peer_id", saved.PeerID)
		q.Set("peer_id_hash", saved.PeerIDHash)
		u.RawQuery = q.Encode()
		wsURL = u.String()
		log.Printf("复用 PairDrop 身份: %s", saved.PeerID)
	}

	log.Printf("正在连接 PairDrop 信令服务器: %s", endpoint)
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second
	conn, _, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return err
	}
	node.wsMu.Lock()
	node.wsConn = conn
	node.wsMu.Unlock()
	cancelClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer cancelClose()
	log.Printf("已连接，等待服务器分配身份...")

	for {
		_ = conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if err := node.handleServerMessage(raw); err != nil {
			log.Printf("处理信令失败: %v", err)
		}
	}
}

func (n *Node) close() {
	n.wsMu.Lock()
	if n.wsConn != nil {
		_ = n.wsConn.Close()
		n.wsConn = nil
	}
	n.wsMu.Unlock()
	n.mu.Lock()
	peers := n.peers
	n.peers = make(map[string]*remotePeer)
	n.mu.Unlock()
	for _, peer := range peers {
		peer.closeIncomingFile()
		_ = peer.pc.Close()
	}
}

func (n *Node) sendWS(message any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	n.wsMu.Lock()
	defer n.wsMu.Unlock()
	if n.wsConn == nil {
		return errors.New("信令连接未建立")
	}
	_ = n.wsConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return n.wsConn.WriteMessage(websocket.TextMessage, data)
}

func (n *Node) handleServerMessage(raw []byte) error {
	var msg SignalMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return fmt.Errorf("无效 JSON: %w", err)
	}

	switch msg.Type {
	case "ping":
		return n.sendWS(map[string]any{"type": "pong"})
	case "ws-config":
		return nil
	case "display-name":
		log.Printf("身份已分配: %s (%s)，设备: %s", msg.DisplayName, msg.PeerID, msg.DeviceName)
		if msg.PeerID != "" && msg.PeerIDHash != "" {
			if err := saveIdentity(identity{PeerID: msg.PeerID, PeerIDHash: msg.PeerIDHash}); err != nil {
				log.Printf("保存 PairDrop 身份失败: %v", err)
			}
		}
		if n.room != "" {
			log.Printf("正在加入公共房间: %s", n.room)
			return n.sendWS(map[string]any{
				"type":            "join-public-room",
				"publicRoomId":    n.room,
				"createIfInvalid": true,
			})
		}
		log.Printf("正在加入同公网 IP 房间；请在同一网络打开 https://pairdrop.net")
		return n.sendWS(map[string]any{"type": "join-ip-room"})
	case "public-room-id-invalid":
		return fmt.Errorf("公共房间 %q 无效", msg.PublicRoom)
	case "peers":
		log.Printf("房间内已有 %d 个设备", len(msg.Peers))
		for _, info := range msg.Peers {
			if !info.RTCSupported {
				continue
			}
			log.Printf("发现设备: %s / %s (%s)", info.Name.DisplayName, info.Name.DeviceName, info.ID)
			if _, err := n.ensurePeer(info.ID, msg.RoomType, msg.RoomID, true); err != nil {
				log.Printf("连接设备失败: %v", err)
			}
		}
		return nil
	case "peer-joined":
		if msg.Peer == nil || !msg.Peer.RTCSupported {
			return nil
		}
		log.Printf("新设备加入: %s / %s (%s)", msg.Peer.Name.DisplayName, msg.Peer.Name.DeviceName, msg.Peer.ID)
		_, err := n.ensurePeer(msg.Peer.ID, msg.RoomType, msg.RoomID, false)
		return err
	case "peer-left":
		n.removePeer(msg.PeerID)
		return nil
	case "signal":
		if msg.Sender == nil {
			return errors.New("signal 缺少 sender")
		}
		peer, err := n.ensurePeer(msg.Sender.ID, msg.RoomType, msg.RoomID, false)
		if err != nil {
			return err
		}
		return n.handleSignal(peer, &msg)
	default:
		log.Printf("收到服务端消息: %s", msg.Type)
		return nil
	}
}

func loadIdentity() (identity, error) {
	data, err := os.ReadFile(identityFile)
	if err != nil {
		return identity{}, err
	}
	var saved identity
	if err := json.Unmarshal(data, &saved); err != nil {
		return identity{}, err
	}
	return saved, nil
}

func saveIdentity(saved identity) error {
	data, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		return err
	}
	temp := identityFile + ".tmp"
	if err := os.WriteFile(temp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(temp, identityFile)
}

func (n *Node) ensurePeer(id, roomType, roomID string, caller bool) (*remotePeer, error) {
	n.mu.Lock()
	if peer := n.peers[id]; peer != nil {
		if roomType != "" {
			peer.mu.Lock()
			peer.roomType, peer.roomID = roomType, roomID
			peer.mu.Unlock()
		}
		n.mu.Unlock()
		return peer, nil
	}
	n.mu.Unlock()

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	})
	if err != nil {
		return nil, err
	}
	peer := &remotePeer{id: id, roomType: roomType, roomID: roomID, pc: pc}

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		ice := candidate.ToJSON()
		if err := n.sendSignal(peer, nil, &ice); err != nil {
			log.Printf("发送 ICE Candidate 失败: %v", err)
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("设备 [%s] WebRTC 状态: %s", id, state)
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			n.removePeerIfCurrent(id, peer)
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) { n.bindDataChannel(peer, dc) })

	n.mu.Lock()
	if existing := n.peers[id]; existing != nil {
		n.mu.Unlock()
		_ = pc.Close()
		return existing, nil
	}
	n.peers[id] = peer
	n.mu.Unlock()

	if caller {
		dc, err := pc.CreateDataChannel("data-channel", nil)
		if err != nil {
			n.removePeer(id)
			return nil, err
		}
		n.bindDataChannel(peer, dc)
		offer, err := pc.CreateOffer(nil)
		if err != nil {
			return nil, err
		}
		if err := pc.SetLocalDescription(offer); err != nil {
			return nil, err
		}
		if err := n.sendSignal(peer, pc.LocalDescription(), nil); err != nil {
			return nil, err
		}
	}
	return peer, nil
}

func (n *Node) sendSignal(peer *remotePeer, sdp *webrtc.SessionDescription, ice *webrtc.ICECandidateInit) error {
	peer.mu.Lock()
	message := struct {
		Type     string                     `json:"type"`
		To       string                     `json:"to"`
		RoomType string                     `json:"roomType"`
		RoomID   string                     `json:"roomId"`
		SDP      *webrtc.SessionDescription `json:"sdp,omitempty"`
		ICE      *webrtc.ICECandidateInit   `json:"ice,omitempty"`
	}{"signal", peer.id, peer.roomType, peer.roomID, sdp, ice}
	peer.mu.Unlock()
	return n.sendWS(message)
}

func (n *Node) handleSignal(peer *remotePeer, msg *SignalMessage) error {
	if msg.SDP != nil {
		if err := peer.pc.SetRemoteDescription(*msg.SDP); err != nil {
			return fmt.Errorf("SetRemoteDescription: %w", err)
		}
		peer.mu.Lock()
		peer.remoteSet = true
		pending := peer.pendingICE
		peer.pendingICE = nil
		peer.mu.Unlock()
		for _, candidate := range pending {
			if err := peer.pc.AddICECandidate(candidate); err != nil {
				log.Printf("添加缓存 ICE Candidate 失败: %v", err)
			}
		}
		if msg.SDP.Type == webrtc.SDPTypeOffer {
			answer, err := peer.pc.CreateAnswer(nil)
			if err != nil {
				return err
			}
			if err := peer.pc.SetLocalDescription(answer); err != nil {
				return err
			}
			return n.sendSignal(peer, peer.pc.LocalDescription(), nil)
		}
		return nil
	}
	if msg.ICE != nil {
		peer.mu.Lock()
		if !peer.remoteSet {
			peer.pendingICE = append(peer.pendingICE, *msg.ICE)
			peer.mu.Unlock()
			return nil
		}
		peer.mu.Unlock()
		return peer.pc.AddICECandidate(*msg.ICE)
	}
	return nil
}

func (n *Node) bindDataChannel(peer *remotePeer, dc *webrtc.DataChannel) {
	peer.mu.Lock()
	peer.dc = dc
	peer.mu.Unlock()
	dc.OnOpen(func() {
		log.Printf("已与设备 [%s] 建立数据通道，可以从网页端发送文件", peer.id)
	})
	dc.OnMessage(func(message webrtc.DataChannelMessage) {
		if err := peer.handleDataMessage(message); err != nil {
			log.Printf("处理来自 [%s] 的数据失败: %v", peer.id, err)
			peer.abortIncomingFileSafely()
		}
	})
}

func (p *remotePeer) handleDataMessage(message webrtc.DataChannelMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !message.IsString {
		return p.writeChunk(message.Data)
	}
	var msg DataMessage
	if err := json.Unmarshal(message.Data, &msg); err != nil {
		return err
	}
	switch msg.Type {
	case "request":
		if len(msg.Header) == 0 {
			return errors.New("文件请求为空")
		}
		p.requestedFiles = append([]FileMeta(nil), msg.Header...)
		p.totalSize = msg.TotalSize
		p.completedBytes = 0
		p.lastProgress = 0
		log.Printf("收到 %d 个文件的发送请求（共 %.2f MB），已自动接受", len(msg.Header), float64(msg.TotalSize)/1024/1024)
		return p.sendJSON(map[string]any{"type": "files-transfer-response", "accepted": true})
	case "header":
		if len(p.requestedFiles) == 0 {
			return errors.New("未收到 request 就收到了文件 header")
		}
		expected := p.requestedFiles[0]
		if expected.Name != msg.Name || expected.Size != msg.Size || msg.Size < 0 {
			return fmt.Errorf("文件信息与请求不一致: %q/%d", msg.Name, msg.Size)
		}
		return p.startFile(FileMeta{Name: msg.Name, Size: msg.Size, Mime: msg.Mime})
	case "partition":
		return p.sendJSON(map[string]any{"type": "partition-received", "offset": msg.Offset})
	case "text":
		decoded, err := base64.StdEncoding.DecodeString(msg.Text)
		if err != nil {
			return err
		}
		log.Printf("收到文本: %s", decoded)
		return p.sendJSON(map[string]any{"type": "message-transfer-complete"})
	case "display-name-changed", "progress":
		return nil
	default:
		log.Printf("忽略数据通道消息: %s", msg.Type)
		return nil
	}
}

func (p *remotePeer) startFile(meta FileMeta) error {
	if p.currentFile != nil {
		return errors.New("上一个文件尚未接收完")
	}
	finalPath, err := availablePath(filepath.Base(meta.Name))
	if err != nil {
		return err
	}
	tempPath := finalPath + ".part"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	p.currentFile = &incomingFile{meta: meta, file: file, tempPath: tempPath, finalPath: finalPath}
	log.Printf("开始接收文件 %q（%.2f MB）", meta.Name, float64(meta.Size)/1024/1024)
	if meta.Size == 0 {
		return p.finishFile()
	}
	return nil
}

func (p *remotePeer) writeChunk(data []byte) error {
	current := p.currentFile
	if current == nil {
		return errors.New("没有活动文件却收到了二进制数据")
	}
	if current.received+int64(len(data)) > current.meta.Size {
		return errors.New("收到的数据超过声明的文件大小")
	}
	written, err := current.file.Write(data)
	current.received += int64(written)
	if err != nil {
		return err
	}
	progress := float64(p.completedBytes+current.received) / float64(p.totalSize)
	if p.totalSize <= 0 {
		progress = 1
	}
	if progress-p.lastProgress >= 0.005 || progress == 1 {
		if err := p.sendJSON(map[string]any{"type": "progress", "progress": progress}); err != nil {
			return err
		}
		p.lastProgress = progress
	}
	if current.received == current.meta.Size {
		return p.finishFile()
	}
	return nil
}

func (p *remotePeer) finishFile() error {
	current := p.currentFile
	if current == nil {
		return nil
	}
	if err := current.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(current.tempPath, current.finalPath); err != nil {
		return err
	}
	log.Printf("文件接收完毕: %s", current.finalPath)
	p.currentFile = nil
	p.completedBytes += current.meta.Size
	progress := float64(p.completedBytes) / float64(p.totalSize)
	if p.totalSize <= 0 || progress > 1 {
		progress = 1
	}
	if progress > p.lastProgress {
		if err := p.sendJSON(map[string]any{"type": "progress", "progress": progress}); err != nil {
			return err
		}
		p.lastProgress = progress
	}
	if len(p.requestedFiles) > 0 {
		p.requestedFiles = p.requestedFiles[1:]
	}
	return p.sendJSON(map[string]any{"type": "file-transfer-complete"})
}

func (p *remotePeer) sendJSON(message any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if p.dc == nil {
		return errors.New("数据通道尚未建立")
	}
	return p.dc.SendText(string(data))
}

func (p *remotePeer) closeIncomingFile() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.abortIncomingFile()
}

func (p *remotePeer) abortIncomingFileSafely() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.abortIncomingFile()
}

func (p *remotePeer) abortIncomingFile() {
	if p.currentFile == nil {
		return
	}
	_ = p.currentFile.file.Close()
	_ = os.Remove(p.currentFile.tempPath)
	p.currentFile = nil
}

func (n *Node) removePeer(id string) {
	n.removePeerIfCurrent(id, nil)
}

func (n *Node) removePeerIfCurrent(id string, expected *remotePeer) {
	n.mu.Lock()
	peer := n.peers[id]
	if expected != nil && peer != expected {
		n.mu.Unlock()
		return
	}
	delete(n.peers, id)
	n.mu.Unlock()
	if peer == nil {
		return
	}
	peer.closeIncomingFile()
	_ = peer.pc.Close()
	log.Printf("设备已离开: %s", id)
}

func availablePath(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == string(filepath.Separator) {
		return "", errors.New("无效文件名")
	}
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	for i := 0; ; i++ {
		candidate := name
		if i > 0 {
			candidate = fmt.Sprintf("%s (%d)%s", base, i, ext)
		}
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			if _, err := os.Stat(candidate + ".part"); errors.Is(err, os.ErrNotExist) {
				return candidate, nil
			}
		}
	}
}
