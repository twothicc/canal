package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/twothicc/common-go/grpcclient"
	"github.com/twothicc/common-go/logger"
	pb "github.com/twothicc/protobuf/datasync/v1"
	"go.uber.org/zap"
)

type SyncEventHandler interface {
	canal.EventHandler
}

type syncEventHandler struct {
	canal.DummyEventHandler
	ctx      context.Context
	client   *grpcclient.Client
	serverId uint32
}

func NewSyncEventHandler(
	ctx context.Context,
	client *grpcclient.Client,
	serverId uint32,
) SyncEventHandler {
	return &syncEventHandler{
		ctx:      ctx,
		client:   client,
		serverId: serverId,
	}
}

func (se *syncEventHandler) OnRow(e *canal.RowsEvent) error {
	logger.WithContext(se.ctx).Info("[SyncEventHandler.OnRow]handling rows event", zap.Uint32("server id", se.serverId))

	if e == nil {
		logger.WithContext(se.ctx).Error(
			"[SyncEventHandler.OnRow]rows event is nil",
			zap.Uint32("server id", se.serverId),
		)

		return ErrEvent.New("[SyncEventHandler.OnRow]rows event is nil")
	}

	resp := &pb.SyncResponse{}

	req, err := se.parseRowsEvent(e)
	if err == nil {
		if callErr := se.client.Call(
			se.ctx,
			"localhost:8080", "/datasync.v1.SyncService/Sync",
			req, resp,
		); callErr != nil {
			logger.WithContext(se.ctx).Error(
				"[SyncEventHandler.OnRow]fail to call",
				zap.Uint32("server id", se.serverId),
				zap.Error(callErr),
			)
		} else {
			logger.WithContext(se.ctx).Info(
				"[SyncEventHandler.OnRow]handled rows event",
				zap.Uint32("server id", se.serverId),
				zap.String("msg", resp.GetMsg()),
			)
		}
	} else {
		logger.WithContext(se.ctx).Warn(
			"[SyncEventHandler.OnRow]fail to handle rows event",
			zap.Uint32("server id", se.serverId),
			zap.Error(err),
		)
	}

	return nil
}

func (se *syncEventHandler) parseRowsEvent(e *canal.RowsEvent) (*pb.SyncRequest, error) {
	// parse primary keys
	pk := []string{}

	for _, idx := range e.Table.Indexes {
		if idx.Name == PRIMARY_KEY {
			pk = append(pk, idx.Columns...)
		}
	}

	// parse old and new row data
	oldRow := make(map[string]interface{})
	newRow := make(map[string]interface{})

	for i := 0; i < len(e.Table.Columns); i++ {
		columnName := e.Table.Columns[i].Name

		if e.Action == INSERT {
			newRow[columnName] = e.Rows[0][i]
		} else {
			oldRow[columnName] = e.Rows[0][i]
			newRow[columnName] = e.Rows[1][i]
		}
	}

	byteOldRow, err := json.Marshal(oldRow)
	if err != nil {
		logger.WithContext(se.ctx).Error(
			"[SyncEventHandler.parseRowsEvent]fail to marshal old data",
			zap.Uint32("server id", se.serverId),
			zap.Any("old data", oldRow),
			zap.Error(err),
		)

		return nil, ErrMarshal.New(fmt.Sprintf("[SyncEventHandler.parseRowsEvent]%s", err.Error()))
	}

	byteNewRow, err := json.Marshal(newRow)
	if err != nil {
		logger.WithContext(se.ctx).Error(
			"[SyncEventHandler.parseRowsEvent]fail to marshal new data",
			zap.Uint32("server id", se.serverId),
			zap.Any("new data", newRow),
			zap.Error(err),
		)

		return nil, ErrMarshal.New(fmt.Sprintf("[SyncEventHandler.parseRowsEvent]%s", err.Error()))
	}

	// parse timestamp
	var (
		ctimestamp uint32
		mtimestamp uint32
	)

	if e.Action == INSERT || e.Action == UPDATE {
		if rawCtime, ok := newRow[CREATE_TIME]; ok {
			ctime, err := se.parseTimestamp(rawCtime)
			if err != nil {
				return nil, err
			}

			ctimestamp = ctime
		}

		if rawMtime, ok := newRow[MODIFY_TIME]; ok {
			mtime, err := se.parseTimestamp(rawMtime)
			if err != nil {
				return nil, err
			}

			mtimestamp = mtime
		}
	}

	return &pb.SyncRequest{
		Ctimestamp: ctimestamp,
		Mtimestamp: mtimestamp,
		Action:     e.Action,
		Schema:     e.Table.Schema,
		Table:      e.Table.Name,
		Pk:         pk,
		OldData:    byteOldRow,
		NewData:    byteNewRow,
	}, nil
}

func (se *syncEventHandler) parseTimestamp(rawTimestamp interface{}) (uint32, error) {
	timestampString := fmt.Sprint(rawTimestamp)
	timestamp, err := strconv.ParseUint(timestampString, 10, 32)
	if err != nil {
		logger.WithContext(se.ctx).Error(
			"[SyncEventHandler.parseTimestamp]fail to parse ctime",
			zap.Uint32("server id", se.serverId),
			zap.String("timestamp", timestampString),
			zap.Error(err),
		)

		return 0, ErrParse.New(fmt.Sprintf("[SyncEventHandler.parseTimestamp]%s", err.Error()))
	}

	return uint32(timestamp), nil
}
