package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/loveuer/esgo2dump/pkg/log"

	"github.com/loveuer/esgo2dump/internal/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	go func() {
		<-ctx.Done()
		log.Fatal(ctx.Err().Error())
	}()

	if err := cmd.Run(ctx); err != nil {
		log.Error(err.Error())
		return
	}
}
