# PairDrop Go receiver

可复用 Go 库，支持同公网 IP / 公共房间发现、自动重连、文件收发、总进度回报和状态快照。
当前支持接收由网页端/其他客户端发来的文件，也可主动向已连接设备发送本地文件；不含持久设备配对或断点续传。

## 使用

```go
client, err := pairdrop.New(pairdrop.Config{
    SaveDir: "downloads",
    Room: "", // 留空同公网 IP；或五字母公共房间
})
if err != nil { return err }
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go client.Run(ctx)
snapshot := client.Snapshot()
_ = snapshot
```

导入路径：`webShare/pairdrop`。库不依赖 GUI，不调用 os.Exit，不改工作目录。
Run 阻塞直到 context 取消；同一实例不允许并发 Run。每个运行实例应使用独立的 IdentityPath。
Snapshot 返回独立副本，可在界面线程定期轮询，包含身份、设备、最近 100 条传输和 150 条日志。
默认身份文件位于用户配置目录 PairDropGo 内；可显式配置 IdentityPath。
接收自动同意；文件写入指定目录，同名文件追加编号，失败的临时文件会清理。
主动发送使用 `client.SendFiles(peerID, paths)`，立即返回、后台传输，进度同样通过 Snapshot 查看（状态为“发送中/已发送”）。
信令断线会重建连接，未完成的文件需要重发；跨网络是否能连接取决于服务端 ICE 配置及网络条件。

## Windows 程序

运行根目录 `./build-gui.ps1`（Go、windres，Windows amd64）。
输出 `dist/PairDropReceiver.exe` 和 `dist/PairDropCLI.exe`。
GUI 为原生 Windows 窗口，无控制台；默认保存到用户 Downloads/PairDrop。
GUI 默认不联网，点击“开始接收”后连接；“停止接收”或关闭窗口取消运行。
连接后可在“发送文件”卡片选择已连接设备与本地文件并点击“发送”。
GUI 使用命名互斥量限制单实例，避免重复显示设备。
命令行：`go run ./cmd/pairdrop-cli -dir ./downloads -room abcde`。
原有根目录 Web 服务器入口保留不变。
