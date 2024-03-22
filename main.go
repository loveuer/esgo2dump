package main

import (
	"context"
	"esgo2dump/internal/cmd"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	if err := cmd.Start(ctx); err != nil {
		logrus.Error(err)
		return
	}

	logrus.Debug("main: cmd start success!!!")
}
