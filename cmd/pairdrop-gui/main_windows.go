package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/dosgo/pairdrop-client/pairdrop"

	"github.com/lxn/walk"
	. "github.com/lxn/walk/declarative"
	"golang.org/x/sys/windows"
)

func main() {
	smoke := flag.Bool("smoke-test", false, "创建窗口后自动退出，用于验证 GUI 打包")
	flag.Parse()
	name, _ := windows.UTF16PtrFromString("Local\\PairDropGoDesktop")
	mutex, err := windows.CreateMutex(nil, false, name)
	if err == windows.ERROR_ALREADY_EXISTS {
		if mutex != 0 {
			windows.CloseHandle(mutex)
		}
		walk.MsgBox(nil, "PairDrop 接收器", "程序已经运行，请使用已打开的窗口。", walk.MsgBoxIconInformation)
		return
	}
	if err != nil {
		os.Exit(1)
	}
	defer windows.CloseHandle(mutex)
	var mw *walk.MainWindow
	var folder, room *walk.LineEdit
	var status *walk.Label
	var devices, transfers, logs *walk.TextEdit
	var start, stop, browse *walk.PushButton
	var client *pairdrop.Node
	var cancel context.CancelFunc
	var done chan error
	userDir, _ := os.UserHomeDir()
	defaultDir := filepath.Join(userDir, "Downloads", "PairDrop")
	showError := func(err error) { walk.MsgBox(mw, "操作失败", err.Error(), walk.MsgBoxIconError) }
	update := func() {
		if client == nil {
			return
		}
		s := client.Snapshot()
		title := s.Status
		if s.Name != "" {
			title += "  ·  本机名称：" + s.Name
		}
		status.SetText(title)
		var ds, ts []string
		for _, d := range s.Devices {
			ds = append(ds, fmt.Sprintf("%s  —  %s", d.Name, d.State))
		}
		if len(ds) == 0 {
			ds = append(ds, "尚未发现设备。请打开 pairdrop.net，或加入相同公共房间。")
		}
		for i := len(s.Transfers) - 1; i >= 0; i-- {
			t := s.Transfers[i]
			percent := float64(0)
			if t.Size > 0 {
				percent = float64(t.Received) * 100 / float64(t.Size)
			} else if t.State == "已完成" {
				percent = 100
			}
			ts = append(ts, fmt.Sprintf("%s  |  %s  %.0f%%  |  %.2f / %.2f MB", t.Name, t.State, percent, float64(t.Received)/1048576, float64(t.Size)/1048576))
		}
		if len(ts) == 0 {
			ts = append(ts, "暂无传输。开始接收后，在网页中选择本机发送文件。")
		}
		set := func(w *walk.TextEdit, text string) {
			if w.Text() != text {
				w.SetText(text)
			}
		}
		set(devices, strings.Join(ds, "\r\n"))
		set(transfers, strings.Join(ts, "\r\n"))
		set(logs, strings.Join(s.Logs, "\r\n"))
		if done != nil {
			select {
			case err := <-done:
				done = nil
				cancel = nil
				start.SetEnabled(true)
				stop.SetEnabled(false)
				folder.SetEnabled(true)
				room.SetEnabled(true)
				browse.SetEnabled(true)
				if err != nil {
					showError(err)
				}
			default:
			}
		}
	}
	err = (MainWindow{
		AssignTo: &mw, Title: "PairDrop 文件接收器", MinSize: Size{780, 640}, Size: Size{900, 780},
		Font:   Font{Family: "Microsoft YaHei UI", PointSize: 10},
		Layout: VBox{Margins: Margins{18, 18, 18, 18}, Spacing: 10},
		Children: []Widget{
			Label{Text: "PairDrop  /  文件接收", Font: Font{Family: "Microsoft YaHei UI", PointSize: 20, Bold: true}},
			Label{Text: "启动后自动接受发现设备发来的文件。关闭窗口将停止接收。"},
			Label{AssignTo: &status, Text: "未连接"},
			Composite{Layout: HBox{}, Children: []Widget{
				Label{Text: "保存到"}, LineEdit{AssignTo: &folder, Text: defaultDir},
				PushButton{AssignTo: &browse, Text: "选择目录…", OnClicked: func() {
					dlg := walk.FileDialog{Title: "选择文件保存目录", FilePath: folder.Text()}
					if ok, err := dlg.ShowBrowseFolder(mw); err != nil {
						showError(err)
					} else if ok {
						folder.SetText(dlg.FilePath)
					}
				}},
			}},
			Composite{Layout: HBox{}, Children: []Widget{
				Label{Text: "公共房间"}, LineEdit{AssignTo: &room, MaxLength: 5},
				Label{Text: "留空：同公网 IP；跨网络：双方输入相同五字母房间"},
			}},
			Composite{Layout: HBox{}, Children: []Widget{
				PushButton{AssignTo: &start, Text: "开始接收", OnClicked: func() {
					c, err := pairdrop.New(pairdrop.Config{SaveDir: strings.TrimSpace(folder.Text()), Room: strings.ToLower(strings.TrimSpace(room.Text()))})
					if err != nil {
						showError(err)
						return
					}
					client = c
					ctx, stopRun := context.WithCancel(context.Background())
					cancel = stopRun
					done = make(chan error, 1)
					result := done
					start.SetEnabled(false)
					stop.SetEnabled(true)
					folder.SetEnabled(false)
					room.SetEnabled(false)
					browse.SetEnabled(false)
					go func() { result <- c.Run(ctx) }()
					update()
				}},
				PushButton{AssignTo: &stop, Text: "停止接收", Enabled: false, OnClicked: func() {
					if cancel != nil {
						cancel()
						stop.SetEnabled(false)
						status.SetText("正在停止…")
					}
				}},
				PushButton{Text: "打开保存目录", OnClicked: func() {
					dir, err := filepath.Abs(folder.Text())
					if err != nil {
						showError(err)
						return
					}
					if err = os.MkdirAll(dir, 0755); err != nil {
						showError(err)
						return
					}
					if err = exec.Command("explorer.exe", dir).Start(); err != nil {
						showError(err)
					}
				}},
			}},
			GroupBox{Title: "设备", Layout: VBox{}, Children: []Widget{TextEdit{AssignTo: &devices, ReadOnly: true, VScroll: true, MinSize: Size{0, 90}}}},
			GroupBox{Title: "传输记录（最近 100 项）", Layout: VBox{}, Children: []Widget{TextEdit{AssignTo: &transfers, ReadOnly: true, VScroll: true, MinSize: Size{0, 130}}}},
			GroupBox{Title: "连接日志", Layout: VBox{}, Children: []Widget{TextEdit{AssignTo: &logs, ReadOnly: true, VScroll: true, MinSize: Size{0, 110}}}},
		},
	}.Create())
	if err != nil {
		if *smoke {
			os.Exit(1)
		}
		walk.MsgBox(nil, "启动失败", err.Error(), walk.MsgBoxIconError)
		os.Exit(1)
	}
	shutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-shutdown:
				return
			case <-ticker.C:
				mw.Synchronize(update)
			}
		}
	}()
	mw.Closing().Attach(func(canceled *bool, reason walk.CloseReason) {
		if cancel != nil {
			cancel()
		}
	})
	if *smoke {
		time.AfterFunc(time.Second, func() { mw.Synchronize(func() { mw.Close() }) })
	}
	mw.Run()
	close(shutdown)
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
}
