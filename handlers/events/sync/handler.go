package sync

import (
	"context"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/twothicc/common-go/grpcclient"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type SyncEventHandler interface {
	canal.EventHandler
}

type syncEventHandler struct {
	canal.DummyEventHandler
	ctx        context.Context
	grpcClient *grpcclient.Client
}

func NewSyncEventHandler(
	ctx context.Context,
	grpcClient *grpcclient.Client,
) SyncEventHandler {
	return &syncEventHandler{
		ctx:        ctx,
		grpcClient: grpcClient,
	}
}

func (se *syncEventHandler) OnRow(e *canal.RowsEvent) error {
	logger.WithContext(se.ctx).Info(
		"[SyncEventHandler.OnRow]handling",
		zap.Any("rows event", &e),
	)

	return nil
}
