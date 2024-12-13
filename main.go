package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/loveuer/nf/nft/log"

	"github.com/loveuer/esgo2dump/internal/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	if err := cmd.Start(ctx); err != nil {
		log.Error(err.Error())
		return
	}
}
