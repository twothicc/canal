package main

import (
	"context"

	"github.com/twothicc/canal/tools/env"
	"github.com/twothicc/common-go/grpcclient"
	"github.com/twothicc/common-go/grpcclient/pool"
)

type dependencies struct {
	grpcClient *grpcclient.Client
}

func initDependencies(ctx context.Context) *dependencies {
	clientConfigs := grpcclient.GetDefaultClientConfigs(
		env.EnvConfigs.ServiceName,
		env.IsTest(),
		pool.PoolCreator(pool.GetDefaultConnPoolConfigs("localhost:8080"), nil, nil),
	)

	client := grpcclient.NewClient(ctx, clientConfigs)

	return &dependencies{
		grpcClient: client,
	}
}
