package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twothicc/canal/domain/entity/syncmanager"
	"github.com/twothicc/canal/infra/httprouter"
	"github.com/twothicc/canal/tools/env"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var ctx = context.Background()

const READ_HEADER_TIMEOUT = 2

func main() {
	logger.InitLogger(zapcore.InfoLevel)

	env.Init(ctx)

	if env.IsTest() {
		logger.InitLogger(zapcore.DebugLevel)
	}

	dependencies := initDependencies(ctx)

	logger.WithContext(ctx).Info("loaded dependencies")

	// TODO read addr from toml config
	httpServer := &http.Server{
		Addr: "localhost:3030",
		Handler: httprouter.NewHTTPRouter(ctx, &httprouter.HttpRouterDependencies{
			GrpcClient:     dependencies.GrpcClient,
			Cfg:            dependencies.AppConfig,
			SyncController: dependencies.SyncController,
		}),
		ReadHeaderTimeout: READ_HEADER_TIMEOUT * time.Second,
	}

	// testing purpose only
	dependencies.AppConfig.DbConfig.Pass = env.EnvConfigs.DbPass

	syncManager, err := syncmanager.NewSyncManager(
		ctx,
		dependencies.AppConfig,
	)
	if err != nil {
		panic(err)
	}

	dependencies.SyncController.Add(ctx, syncManager.GetId(), syncManager)

	if err := dependencies.SyncController.Start(ctx, syncManager.GetId(), false); err != nil {
		logger.WithContext(ctx).Error("fail to run canal", zap.Error(err))
	}

	ListenSignals(ctx, httpServer, dependencies)
}

func ListenSignals(ctx context.Context, httpServer *http.Server, d *Dependencies) {
	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	sig := <-signalChan

	logger.WithContext(ctx).Info("receive signal, stopping server", zap.String("signal", sig.String()))
	time.Sleep(1 * time.Second)

	d.GrpcClient.Close(ctx)
	logger.WithContext(ctx).Info("[Main.ListenSignals]closed grpc clients")

	if err := d.SyncController.Close(ctx); err != nil {
		logger.WithContext(ctx).Error("[Main.ListenSignals]fail to close sync controller", zap.Error(err))
	} else {
		logger.WithContext(ctx).Info("[Main.ListenSignals]closed sync controller")
	}

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.WithContext(ctx).Error("[Main.ListenSignals]fail to gracefully shutdown http server", zap.Error(err))
	} else {
		logger.WithContext(ctx).Info("[Main.ListenSignals]gracefully shutdown http server")
	}

	logger.Sync()
}
