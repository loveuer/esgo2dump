package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/loveuer/esgo2dump/pkg/log"

	"github.com/loveuer/esgo2dump/internal/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			log.Warn("Process interrupted")
			os.Exit(1)
		case <-done:
		}
	}()

	if err := cmd.Run(ctx); err != nil {
		log.Error(err.Error())
	}

	// stop the interruption watcher before deferred cancel() fires,
	// otherwise every normal exit would log "Process interrupted"
	close(done)
}
