package sync

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/twothicc/canal/config"
	"github.com/twothicc/canal/handlers/events/kafka"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type SyncEventHandler interface {
	canal.EventHandler
}

type syncEventHandler struct {
	canal.DummyEventHandler
	ctx         context.Context
	msgProducer kafka.IMessageProducer
	syncCh      chan mysql.Position
	serverId    uint32
}

type CloseEventHandler func() error

func NewSyncEventHandler(
	ctx context.Context,
	kafkaCfg config.KafkaConfig,
	serverId uint32,
	syncCh chan mysql.Position,
) (SyncEventHandler, CloseEventHandler, error) {
	msgProducer, err := kafka.NewMessageProducer(ctx, kafkaCfg)
	if err != nil {
		return nil, nil, ErrConstructor.Wrap(err)
	}

	return &syncEventHandler{
			ctx:         ctx,
			msgProducer: msgProducer,
			serverId:    serverId,
			syncCh:      syncCh,
		}, func() error {
			if closeErr := msgProducer.Close(); closeErr != nil {
				logger.WithContext(ctx).Error(
					"[NewSyncEventHandler]fail to close message producer. Possible memory leak",
				)

				return closeErr
			}

			return nil
		}, nil
}

func (se *syncEventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	se.syncCh <- pos

	return se.ctx.Err()
}

func (se *syncEventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	se.syncCh <- nextPos
	return se.ctx.Err()
}

func (se *syncEventHandler) OnXID(nextPos mysql.Position) error {
	se.syncCh <- nextPos
	return se.ctx.Err()
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

	msg, err := se.parseRowsEvent(e)
	if err == nil {
		se.msgProducer.Produce(se.ctx, msg)
	}

	return nil
}

func (se *syncEventHandler) parseRowsEvent(e *canal.RowsEvent) (kafka.IMessage, error) {
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

	return &kafka.SyncMessage{
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
	timestampFloat, ok := rawTimestamp.(float64)
	if !ok {
		logger.WithContext(se.ctx).Error(
			"[SyncEventHandler.parseTimestamp]fail to parse ctime",
			zap.Uint32("server id", se.serverId),
			zap.Any("timestamp", rawTimestamp),
		)

		return 0, ErrParse.New("[SyncEventHandler.parseTimestamp]fail to parse ctime")
	}

	return uint32(timestampFloat), nil
}
