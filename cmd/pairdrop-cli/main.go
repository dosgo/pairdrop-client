package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/dosgo/pairdrop-client/pairdrop"
)

func main() {
	room := flag.String("room", "", "五字母公共房间（留空：同公网 IP）")
	dir := flag.String("dir", ".", "文件保存目录")
	identity := flag.String("identity", ".pairdrop-identity.json", "身份文件")
	flag.Parse()
	client, err := pairdrop.New(pairdrop.Config{Room: strings.ToLower(*room), SaveDir: *dir, IdentityPath: *identity})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	last := ""
	for {
		select {
		case err := <-done:
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
			return
		case <-ticker.C:
			logs := client.Snapshot().Logs
			start := 0
			for i := len(logs) - 1; i >= 0; i-- {
				if logs[i] == last {
					start = i + 1
					break
				}
			}
			for _, line := range logs[start:] {
				fmt.Println(line)
			}
			if len(logs) > 0 {
				last = logs[len(logs)-1]
			}
		}
	}
}
