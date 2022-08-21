package synccontroller

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/twothicc/canal/domain/entity/syncmanager"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type SyncController interface {
	Add(ctx context.Context, id uint32, manager syncmanager.SyncManager)
	Remove(ctx context.Context, id uint32) error

	Start(ctx context.Context, id uint32, isLegacySync bool) error
	Stop(ctx context.Context, id uint32) error

	Status() map[uint32]*syncmanager.Status

	Close(ctx context.Context) error
}

type syncController struct {
	syncmanagers map[uint32]syncmanager.SyncManager
	mu           sync.Mutex
}

func NewSyncController(_ context.Context) SyncController {
	return &syncController{
		syncmanagers: make(map[uint32]syncmanager.SyncManager),
	}
}

func (s *syncController) Status() map[uint32]*syncmanager.Status {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[uint32]*syncmanager.Status)

	for id, manager := range s.syncmanagers {
		res[id] = manager.Status()
	}

	return res
}

func (s *syncController) Add(ctx context.Context, id uint32, manager syncmanager.SyncManager) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger.WithContext(ctx).Info("[SyncManager.Add]adding syncmanager", zap.Uint32("id", id))
	s.syncmanagers[id] = manager
}

func (s *syncController) Remove(ctx context.Context, id uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger.WithContext(ctx).Info("[SyncManager.Remove]removing syncmanager", zap.Uint32("id", id))

	if manager, ok := s.syncmanagers[id]; !ok {
		return ErrParam.New(fmt.Sprintf("[SyncManager.Remove]id %d does not exist", id))
	} else {
		if manager.Status().IsRunning {
			manager.Close()
		}

		delete(s.syncmanagers, id)

		if err := os.Remove(fmt.Sprintf("canal%d.log", manager.GetId())); err != nil {
			logger.WithContext(ctx).Error(
				"[SyncManager.Remove]fail to delete log file",
				zap.Uint32("server id", manager.GetId()),
			)
		}
	}

	return nil
}

func (s *syncController) Start(ctx context.Context, id uint32, isLegacySync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if manager, ok := s.syncmanagers[id]; !ok {
		return ErrParam.New(fmt.Sprintf("[SyncManager.Start]id %d does not exist", id))
	} else {
		if !manager.Status().IsRunning {
			go manager.Run(isLegacySync)
		}
	}

	return nil
}

func (s *syncController) Stop(ctx context.Context, id uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if manager, ok := s.syncmanagers[id]; !ok {
		return ErrParam.New(fmt.Sprintf("[SyncManager.Stop]id %d does not exist", id))
	} else {
		if manager.Status().IsRunning {
			manager.Close()
		}
	}

	return nil
}

func (s *syncController) Close(ctx context.Context) error {
	logger.WithContext(ctx).Info("[SyncManager.Close]closing all syncmanagers")

	for key, _ := range s.syncmanagers {
		if err := s.Remove(ctx, key); err != nil {
			logger.WithContext(ctx).Error("[SyncManager.Close]fail to close syncmanagers", zap.Error(err))

			return err
		}
	}

	return nil
}
