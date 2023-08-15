package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/component-base/cli"

	"github.com/clusterrouter-io/clusterrouter/cmd/virtualnode-manager/app"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	command := app.NewVirtualNodeManagerCommand(ctx)
	code := cli.Run(command)
	os.Exit(code)
}
