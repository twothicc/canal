package main

import (
	"context"

	"github.com/twothicc/canal/config"
	"github.com/twothicc/canal/domain/entity/syncmanager"
	"github.com/twothicc/canal/tools/env"
	"github.com/twothicc/common-go/grpcclient"
	"github.com/twothicc/common-go/grpcclient/pool"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type dependencies struct {
	grpcClient   *grpcclient.Client
	appConfig    *config.Config
	canalManager syncmanager.SyncManager
}

func initDependencies(ctx context.Context) *dependencies {
	appConfig, err := config.NewConfig("./conf/app.toml")
	if err != nil {
		logger.WithContext(ctx).Error("[initDependencies]fail to load config", zap.Error(err))
	}

	clientConfigs := grpcclient.GetDefaultClientConfigs(
		env.EnvConfigs.ServiceName,
		env.IsTest(),
		pool.PoolCreator(pool.GetDefaultConnPoolConfigs("localhost:8080"), nil, nil),
	)

	client := grpcclient.NewClient(ctx, clientConfigs)

	canalManager := syncmanager.NewSyncManager(
		ctx,
		appConfig,
		client,
	)

	return &dependencies{
		grpcClient:   client,
		appConfig:    appConfig,
		canalManager: canalManager,
	}
}
