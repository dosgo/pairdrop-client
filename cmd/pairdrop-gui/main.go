package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dosgo/pairdrop-client/pairdrop"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"golang.org/x/sys/windows"
)

const (
	appTitle  = "PairDrop 文件收发器"
	mutexName = "Local\\PairDropGoDesktop"
)

func main() {
	smoke := flag.Bool("smoke-test", false, "创建窗口后自动退出，用于验证 GUI 打包")
	flag.Parse()

	name, _ := windows.UTF16PtrFromString(mutexName)
	mutex, err := windows.CreateMutex(nil, false, name)
	if err == windows.ERROR_ALREADY_EXISTS {
		if mutex != 0 {
			windows.CloseHandle(mutex)
		}
		if !*smoke {
			showMessage("PairDrop 接收器", "程序已经运行，请使用已打开的窗口。")
		}
		return
	}
	if err != nil {
		os.Exit(1)
	}
	defer windows.CloseHandle(mutex)

	a := app.NewWithID("net.pairdrop.go.desktop")
	w := a.NewWindow(appTitle)
	setupUI(w, *smoke)
}

// showMessage 弹出一次性的提示窗口，用于“程序已运行”等场景。
func showMessage(title, message string) {
	a := app.New()
	w := a.NewWindow(title)
	w.SetContent(container.NewVBox(
		widget.NewLabel(message),
		widget.NewButton("确定", func() { w.Close() }),
	))
	w.Resize(fyne.NewSize(380, 160))
	w.CenterOnScreen()
	w.ShowAndRun()
}

func setupUI(w fyne.Window, smoke bool) {
	userDir, _ := os.UserHomeDir()
	defaultDir := filepath.Join(userDir, "Downloads", "PairDrop")

	var (
		client   *pairdrop.Node
		cancel   context.CancelFunc
		done     chan error
		shutdown = make(chan struct{})
		stopOnce sync.Once
	)

	statusLabel := widget.NewLabel("未启动：点击「开始接收」后才会连接 PairDrop 服务器。")
	statusLabel.TextStyle = fyne.TextStyle{Bold: true}

	folderEntry := widget.NewEntry()
	folderEntry.SetText(defaultDir)
	roomEntry := widget.NewEntry()
	roomEntry.SetPlaceHolder("五个小写字母，如 abcde")

	devicesText := widget.NewLabel("尚未发现设备。开始接收后，在同一网络打开 https://pairdrop.net，或加入相同公共房间。")
	devicesText.Wrapping = fyne.TextWrapWord
	transfersText := widget.NewLabel("暂无传输记录。")
	transfersText.Wrapping = fyne.TextWrapWord
	logsText := widget.NewLabel("等待启动……")
	logsText.Wrapping = fyne.TextWrapWord
	logsText.TextStyle = fyne.TextStyle{Monospace: true}

	devicesScroll := container.NewVScroll(devicesText)
	devicesScroll.SetMinSize(fyne.NewSize(0, 120))
	transfersScroll := container.NewVScroll(transfersText)
	transfersScroll.SetMinSize(fyne.NewSize(0, 150))
	logsScroll := container.NewVScroll(logsText)
	logsScroll.SetMinSize(fyne.NewSize(0, 140))

	startBtn := widget.NewButtonWithIcon("开始接收", theme.MediaPlayIcon(), nil)
	startBtn.Importance = widget.HighImportance
	stopBtn := widget.NewButtonWithIcon("停止接收", theme.MediaStopIcon(), nil)
	stopBtn.Disable()
	browseBtn := widget.NewButtonWithIcon("选择目录…", theme.FolderOpenIcon(), nil)
	openBtn := widget.NewButtonWithIcon("打开目录", theme.FolderIcon(), nil)

	// 发送区：选择已连接设备与本地文件后点击发送。
	var (
		deviceIDs      []string
		deviceLabels   []string
		selectedDevice string
		pendingFiles   []string
	)
	targetSelect := widget.NewSelect(nil, func(label string) {
		selectedDevice = ""
		for i, l := range deviceLabels {
			if l == label && i < len(deviceIDs) {
				selectedDevice = deviceIDs[i]
				break
			}
		}
	})
	targetSelect.PlaceHolder = "（未连接设备）"
	pendingLabel := widget.NewLabel("尚未选择文件。")
	pendingLabel.Wrapping = fyne.TextWrapWord
	addFileBtn := widget.NewButtonWithIcon("选择文件…", theme.DocumentIcon(), nil)
	clearFileBtn := widget.NewButtonWithIcon("清空", theme.ContentClearIcon(), nil)
	sendBtn := widget.NewButtonWithIcon("发送", theme.MailSendIcon(), nil)
	sendBtn.Importance = widget.HighImportance

	updatePending := func() {
		if len(pendingFiles) == 0 {
			pendingLabel.SetText("尚未选择文件。")
			return
		}
		names := make([]string, 0, len(pendingFiles))
		for _, path := range pendingFiles {
			names = append(names, filepath.Base(path))
		}
		pendingLabel.SetText(fmt.Sprintf("待发送 %d 个文件：%s", len(pendingFiles), strings.Join(names, "、")))
	}

	// refresh 只在主线程调用，读取线程安全的 Snapshot 并刷新界面。
	refresh := func() {
		if client == nil {
			return
		}
		s := client.Snapshot()

		title := s.Status
		if s.Name != "" {
			title += "   ·   本机名称：" + s.Name
		}
		statusLabel.SetText(title)

		lines := make([]string, 0, len(s.Devices))
		for _, d := range s.Devices {
			name := d.Name
			if name == "" {
				name = d.ID
			}
			lines = append(lines, fmt.Sprintf("%s  —  %s", name, d.State))
		}
		if len(lines) == 0 {
			lines = append(lines, "尚未发现设备。请在 https://pairdrop.net 打开网页，或加入相同公共房间。")
		}
		devicesText.SetText(strings.Join(lines, "\n"))

		// 刷新可发送设备下拉框。
		ids := make([]string, 0, len(s.Devices))
		labels := make([]string, 0, len(s.Devices))
		for _, d := range s.Devices {
			if d.State != "可收发文件" {
				continue
			}
			label := d.Name
			if label == "" {
				label = d.ID
			}
			ids = append(ids, d.ID)
			labels = append(labels, fmt.Sprintf("%s (%s)", label, d.ID))
		}
		if !sameStrings(deviceLabels, labels) {
			deviceIDs, deviceLabels = ids, labels
			targetSelect.Options = labels
			targetSelect.Refresh()
			targetSelect.ClearSelected()
			selectedDevice = ""
			if len(labels) > 0 {
				targetSelect.SetSelectedIndex(0)
			}
		}

		lines = lines[:0]
		for i := len(s.Transfers) - 1; i >= 0; i-- {
			t := s.Transfers[i]
			percent := float64(0)
			if t.Size > 0 {
				percent = float64(t.Received) * 100 / float64(t.Size)
			} else if t.State == "已完成" || t.State == "已发送" {
				percent = 100
			}
			lines = append(lines, fmt.Sprintf("%s  |  %s  %.0f%%  |  %.2f / %.2f MB",
				t.Name, t.State, percent, float64(t.Received)/1048576, float64(t.Size)/1048576))
		}
		if len(lines) == 0 {
			lines = append(lines, "暂无传输。接收时在网页中选择本机发送文件；发送时在上方选择设备与文件。")
		}
		transfersText.SetText(strings.Join(lines, "\n"))

		logsText.SetText(strings.Join(s.Logs, "\n"))

		if done != nil {
			select {
			case err := <-done:
				done = nil
				cancel = nil
				client = nil
				startBtn.Enable()
				stopBtn.Disable()
				folderEntry.Enable()
				roomEntry.Enable()
				browseBtn.Enable()
				deviceIDs, deviceLabels, selectedDevice = nil, nil, ""
				targetSelect.Options = nil
				targetSelect.ClearSelected()
				targetSelect.Refresh()
				if err != nil {
					dialog.ShowError(err, w)
				}
			default:
			}
		}
	}

	startRun := func() {
		c, err := pairdrop.New(pairdrop.Config{
			SaveDir: strings.TrimSpace(folderEntry.Text),
			Room:    strings.ToLower(strings.TrimSpace(roomEntry.Text)),
		})
		if err != nil {
			dialog.ShowError(err, w)
			return
		}
		client = c
		ctx, stopRun := context.WithCancel(context.Background())
		cancel = stopRun
		done = make(chan error, 1)
		result := done
		startBtn.Disable()
		stopBtn.Enable()
		folderEntry.Disable()
		roomEntry.Disable()
		browseBtn.Disable()
		statusLabel.SetText("正在启动，详见下方连接日志……")
		go func() { result <- c.Run(ctx) }()
		refresh()
	}

	startBtn.OnTapped = startRun
	stopBtn.OnTapped = func() {
		if cancel != nil {
			cancel()
			stopBtn.Disable()
			statusLabel.SetText("正在停止……")
		}
	}
	browseBtn.OnTapped = func() {
		chooser := dialog.NewFolderOpen(func(uri fyne.ListableURI, err error) {
			if err != nil {
				dialog.ShowError(err, w)
				return
			}
			if uri != nil {
				folderEntry.SetText(uri.Path())
			}
		}, w)
		chooser.Show()
	}
	openBtn.OnTapped = func() {
		dir, err := filepath.Abs(strings.TrimSpace(folderEntry.Text))
		if err != nil {
			dialog.ShowError(err, w)
			return
		}
		if err = os.MkdirAll(dir, 0755); err != nil {
			dialog.ShowError(err, w)
			return
		}
		if err = exec.Command("explorer.exe", dir).Start(); err != nil {
			dialog.ShowError(err, w)
		}
	}

	addFileBtn.OnTapped = func() {
		chooser := dialog.NewFileOpen(func(reader fyne.URIReadCloser, err error) {
			if err != nil {
				dialog.ShowError(err, w)
				return
			}
			if reader == nil {
				return
			}
			path := reader.URI().Path()
			_ = reader.Close()
			for _, existing := range pendingFiles {
				if existing == path {
					return
				}
			}
			pendingFiles = append(pendingFiles, path)
			updatePending()
		}, w)
		chooser.Show()
	}
	clearFileBtn.OnTapped = func() {
		pendingFiles = nil
		updatePending()
	}
	sendBtn.OnTapped = func() {
		if client == nil {
			dialog.ShowError(errors.New("请先点击「开始接收」启动客户端"), w)
			return
		}
		if selectedDevice == "" {
			dialog.ShowError(errors.New("请先选择目标设备"), w)
			return
		}
		if len(pendingFiles) == 0 {
			dialog.ShowError(errors.New("请先选择要发送的文件"), w)
			return
		}
		if err := client.SendFiles(selectedDevice, append([]string(nil), pendingFiles...)); err != nil {
			dialog.ShowError(err, w)
			return
		}
		pendingFiles = nil
		updatePending()
	}

	title := widget.NewRichText(&widget.TextSegment{
		Style: widget.RichTextStyle{SizeName: theme.SizeNameSubHeadingText, TextStyle: fyne.TextStyle{Bold: true}},
		Text:  appTitle,
	})
	header := container.NewVBox(
		title,
		widget.NewLabel("启动后可在「发送文件」中选择设备与文件发送，也会自动接收对端发来的文件；关闭窗口即停止。"),
		statusLabel,
		container.NewBorder(nil, nil, widget.NewLabel("保存到"), container.NewHBox(browseBtn, openBtn), folderEntry),
		container.NewBorder(nil, nil, widget.NewLabel("公共房间"), nil, roomEntry),
		widget.NewLabel("留空＝同一公网 IP 内互传；跨网络请双方填写相同的五个小写字母房间。"),
		container.NewHBox(startBtn, stopBtn),
		widget.NewSeparator(),
	)
	body := container.NewVBox(
		widget.NewCard("发送文件", "选择已连接设备与本地文件后点击发送", container.NewVBox(
			container.NewBorder(nil, nil, widget.NewLabel("目标设备"), nil, targetSelect),
			container.NewBorder(nil, nil, nil, container.NewHBox(addFileBtn, clearFileBtn), pendingLabel),
			container.NewHBox(sendBtn),
		)),
		widget.NewCard("设备", "已发现/已连接的设备与状态", devicesScroll),
		widget.NewCard("传输记录", "最近 100 项", transfersScroll),
		widget.NewCard("连接日志", "最近 150 条", logsScroll),
	)

	w.SetContent(container.NewBorder(header, nil, nil, nil, body))
	w.Resize(fyne.NewSize(960, 840))
	w.CenterOnScreen()

	shutdownOnce := func() {
		stopOnce.Do(func() {
			if cancel != nil {
				cancel()
			}
			close(shutdown)
		})
	}
	w.SetCloseIntercept(func() {
		shutdownOnce()
		w.Close()
	})

	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-shutdown:
				return
			case <-ticker.C:
				fyne.Do(refresh)
			}
		}
	}()

	if smoke {
		time.AfterFunc(time.Second, func() { fyne.Do(func() { w.Close() }) })
	}
	w.ShowAndRun()
	shutdownOnce()
}

// sameStrings 判断两个字符串切片是否完全一致，用于避免重复刷新下拉框。
func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
