package main

import (
	"context"

	"github.com/twothicc/canal/tools/env"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	pb "github.com/twothicc/protobuf/datasync/v1"
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

	for i := 0; i < 1000; i++ {
		client := dependencies.grpcClient
		resp := &pb.HelloWorldResponse{}

		if err := client.Call(
			ctx,
			"localhost:8080",
			"/datasync.v1.HelloWorldService/HelloWorld",
			&pb.HelloWorldRequest{},
			resp,
		); err != nil {
			logger.WithContext(ctx).Error("fail to call HelloWorldService", zap.Error(err))
		}

		logger.WithContext(ctx).Info("test", zap.String("msg", resp.GetMsg()))
	}
}
