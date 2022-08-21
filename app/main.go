package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twothicc/canal/domain/entity/syncmanager"
	"github.com/twothicc/canal/tools/env"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var ctx = context.Background()

func main() {
	logger.InitLogger(zapcore.InfoLevel)

	env.Init(ctx)

	if env.IsTest() {
		logger.InitLogger(zapcore.DebugLevel)
	}

	dependencies := initDependencies(ctx)

	go dependencies.grpcClient.ListenSignals(ctx)

	logger.WithContext(ctx).Info("loaded dependencies")

	// testing purpose only
	syncManager := syncmanager.NewSyncManager(
		ctx,
		dependencies.appConfig,
		dependencies.grpcClient,
	)

	dependencies.syncController.Add(ctx, syncManager.GetId(), syncManager)

	if err := dependencies.syncController.Start(ctx, syncManager.GetId(), true); err != nil {
		logger.WithContext(ctx).Error("fail to run canal", zap.Error(err))
	}

	ListenSignals(ctx, dependencies)
}

func ListenSignals(ctx context.Context, d *dependencies) {
	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	sig := <-signalChan

	logger.WithContext(ctx).Info("receive signal, stopping server", zap.String("signal", sig.String()))
	time.Sleep(1 * time.Second)

	if err := d.syncController.Close(ctx); err != nil {
		logger.WithContext(ctx).Error("[Dependencies.ListenSignals]fail to close server manager", zap.Error(err))
	}

	logger.Sync()
}
