package syncmanager

import (
	"context"
	"fmt"
	"os"
	"time"

	"regexp"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"github.com/twothicc/canal/config"
	"github.com/twothicc/canal/domain/entity/syncmanager/savemanager"
	"github.com/twothicc/canal/handlers/events/sync"
	"github.com/twothicc/canal/tools/idgenerator"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type Status struct {
	Sources   []config.SourceConfig
	ServerId  uint32
	IsRunning bool
}

// SyncManager - manages data sync
type SyncManager interface {
	Run(isLegacySync bool) error
	Close()
	GetId() uint32
	Status() *Status
}

type syncManager struct {
	ctx               context.Context
	eventHandler      sync.SyncEventHandler
	saveInfo          savemanager.ISaveInfo
	closeEventHandler sync.CloseEventHandler
	cancel            context.CancelFunc
	cfg               *config.Config
	canal             *canal.Canal
	syncCh            chan mysql.Position
	isRunning         bool
}

// NewSyncManager - creates a SyncManager
func NewSyncManager(
	ctx context.Context,
	cfg *config.Config,
) (SyncManager, error) {
	cfg.ServerId = idgenerator.GetId()

	canalCfg := parseCanalCfg(ctx, cfg)

	newCanal, err := canal.NewCanal(canalCfg)
	if err != nil {
		logger.WithContext(ctx).Error("[SyncManager.Run]fail to initialize canal", zap.Error(err))

		return nil, ErrConfig.New(fmt.Sprintf("[SyncManager.Run]%s", err.Error()))
	}

	if err = parseSource(ctx, cfg, newCanal); err != nil {
		logger.WithContext(ctx).Error(
			"[SyncManager.Run]fail to parse source",
			zap.Uint32("server id", cfg.ServerId),
			zap.Error(err),
		)

		return nil, err
	}

	if binErr := newCanal.CheckBinlogRowImage("FULL"); err != nil {
		logger.WithContext(ctx).Error(
			"[SyncManager.Run]invalid binlog row image",
			zap.Uint32("server id", cfg.ServerId),
			zap.Error(binErr),
		)

		return nil, ErrBinlog.New(fmt.Sprintf("[SyncManager.Run]%s", err.Error()))
	}

	syncCh := make(chan mysql.Position, SYNC_CHANNEL_SIZE)

	saveInfo, saveErr := savemanager.LoadSaveInfo(ctx, cfg.ServerId)
	if saveErr != nil {
		logger.WithContext(ctx).Error(
			"[SyncManager.Run]fail to load save info",
			zap.Uint32("server id", cfg.ServerId),
			zap.Error(err),
		)

		return nil, ErrSave.New(fmt.Sprintf("[SyncManager.Run]%s", err.Error()))
	}

	ctx, cancel := context.WithCancel(ctx)

	eventHandler, closeEventHandler, eventHandlerErr := sync.NewSyncEventHandler(
		ctx, cfg.KafkaConfig, cfg.ServerId, syncCh)
	if eventHandlerErr != nil {
		logger.WithContext(ctx).Error(fmt.Sprintf("[SyncManager.Run]%s", eventHandlerErr.Error()))
		cancel()

		return nil, ErrEvent.Wrap(eventHandlerErr)
	}

	newCanal.SetEventHandler(eventHandler)

	return &syncManager{
		isRunning:         false,
		ctx:               ctx,
		cancel:            cancel,
		closeEventHandler: closeEventHandler,
		cfg:               cfg,
		canal:             newCanal,
		saveInfo:          saveInfo,
		syncCh:            syncCh,
	}, nil
}

// Status - returns bool indicating whether syncmanager is running
func (sm *syncManager) Status() *Status {
	return &Status{
		ServerId:  sm.cfg.ServerId,
		IsRunning: sm.isRunning,
		Sources:   sm.cfg.Sources,
	}
}

// GetId - returns the unique server id of this syncmanager
func (sm *syncManager) GetId() uint32 {
	return sm.cfg.ServerId
}

// Run - starts data sync
//
// isLegacySync indicates whether the sync will include existing records
func (sm *syncManager) Run(isLegacySync bool) error {
	logger.WithContext(sm.ctx).Info("[SyncManager.Run]running data sync", zap.Uint32("server id", sm.cfg.ServerId))

	sm.isRunning = true

	go sm.syncLoop(mysql.Position{
		Name: sm.saveInfo.Position().Name,
		Pos:  sm.saveInfo.Position().Pos,
	})

	return func() error {
		var err error

		if isLegacySync {
			if runErr := sm.canal.Run(); runErr != nil {
				err = runErr

				sm.cancel()
			}
		} else {
			pos := sm.saveInfo.Position()

			if runErr := sm.canal.RunFrom(pos); runErr != nil {
				err = runErr

				sm.cancel()
			}
		}

		sm.isRunning = false

		return err
	}()
}

// Close - closes underlying canal, stopping data sync immediately
func (sm *syncManager) Close() {
	logger.WithContext(sm.ctx).Info("[SyncManager.Close]closing", zap.Uint32("server id", sm.cfg.ServerId))

	sm.isRunning = false

	sm.cancel()

	if saveErr := sm.saveInfo.Close(sm.ctx); saveErr != nil {
		logger.WithContext(sm.ctx).Error(
			"[SyncManager.Close]fail to save saveInfo",
			zap.Error(saveErr),
			zap.Uint32("server id", sm.cfg.ServerId),
		)
	}

	if eventErr := sm.closeEventHandler(); eventErr != nil {
		logger.WithContext(sm.ctx).Error(
			"[SyncManager.Close]fail to close eventHandler",
			zap.Error(eventErr),
			zap.Uint32("server id", sm.cfg.ServerId),
		)
	}

	sm.canal.Close()
}

// syncLoop - saves binlog position to file in intervals
func (sm *syncManager) syncLoop(initPos mysql.Position) {
	ticker := time.NewTicker(SAVE_INTERVAL)
	defer ticker.Stop()

	currPos := initPos

	for {
		isSavePos := false

		select {
		case pos := <-sm.syncCh:
			currPos = pos
		case <-ticker.C:
			isSavePos = true
		case <-sm.ctx.Done():
			return
		}

		if isSavePos {
			if err := sm.saveInfo.Save(sm.ctx, currPos); err != nil {
				logger.WithContext(sm.ctx).Error("[SyncManager.syncLoop]fail to save", zap.Error(err))
				sm.cancel()

				return
			}

			isSavePos = false
		}
	}
}

// parseSource - parses special characters in tables from config source into valid tables
func parseSource(ctx context.Context, cfg *config.Config, c *canal.Canal) error {
	logger.WithContext(ctx).Info("[SyncManager.parseSource]parsing source", zap.Uint32("server id", cfg.ServerId))

	if c == nil {
		logger.WithContext(ctx).Error("[SyncManager.parseSource]canal not initialized")

		return ErrNoCanal.New("[SyncManager.parseSource]canal not initialized")
	}

	wildCardTables := make(map[string][]string, len(cfg.Sources))

	for _, source := range cfg.Sources {
		if !isValidTable(source.Tables) {
			logger.WithContext(ctx).Error(
				"[SyncManager.parseSource]invalid tables",
				zap.Strings("tables", source.Tables),
			)

			return ErrConfig.New("[SyncManager.parseSource]invalid tables")
		}

		for _, table := range source.Tables {
			// QuoteMeta escapes regex metacharacters
			if regexp.QuoteMeta(table) != table {
				key := sourceKey(source.Schema, table)

				if _, ok := wildCardTables[key]; ok {
					logger.WithContext(ctx).Error(
						"[SyncManager.parseSource]duplicate wildcard table",
						zap.String("source key", key),
					)

					return ErrConfig.New(fmt.Sprintf("[SyncManager.parseSource]duplicate wildcard table %s", key))
				}

				tableParam := table
				if table == WILDCARD {
					tableParam = ANY_TABLE
				}

				res, err := c.Execute(WILDCARD_TABLE_SQL, tableParam, source.Schema)
				if err != nil {
					logger.WithContext(ctx).Error(
						"[SyncManager.parseSource]fail to query table info",
						zap.String("raw sql", fmt.Sprintf(WILDCARD_TABLE_SQL, tableParam, source.Schema)),
						zap.Error(err),
					)

					return ErrQuery.New(fmt.Sprintf("[SyncManager.parseSource]%s", err.Error()))
				}

				tables := []string{}

				for rowNum := 0; rowNum < res.Resultset.RowNumber(); rowNum++ {
					tableName, _ := res.GetString(rowNum, 0)
					tables = append(tables, tableName)
				}

				c.AddDumpTables(source.Schema, tables...)

				wildCardTables[key] = tables
			} else {
				c.AddDumpTables(source.Schema, table)
			}
		}
	}

	return nil
}

func parseCanalCfg(ctx context.Context, cfg *config.Config) *canal.Config {
	logger.WithContext(ctx).Info(
		"[SyncManager.parseCanalCfg]parsing canal configs",
		zap.Uint32("server id", cfg.ServerId),
	)

	canalCfg := canal.NewDefaultConfig()

	dbCfg := cfg.DbConfig
	canalCfg.Addr = dbCfg.Addr
	canalCfg.User = dbCfg.User
	canalCfg.Password = dbCfg.Pass
	canalCfg.Charset = dbCfg.Charset

	canalCfg.ServerID = cfg.ServerId

	// Set timestamp location to UTC instead of local time
	canalCfg.ParseTime = false
	canalCfg.TimestampStringLocation = time.UTC

	canalLogger, err := initLogger(ctx, cfg)
	if err != nil {
		nullHandler, _ := log.NewNullHandler()
		canalLogger = log.NewDefault(nullHandler)
	}

	canalCfg.Logger = canalLogger

	dumpCfg := cfg.DumpConfig
	canalCfg.Dump.ExecutionPath = dumpCfg.DumpExecPath

	for _, source := range cfg.Sources {
		for _, table := range source.Tables {
			canalCfg.IncludeTableRegex = append(
				canalCfg.IncludeTableRegex,
				fmt.Sprintf("%s\\.%s", source.Schema, table),
			)
		}
	}

	return canalCfg
}

// isValidTable - checks if tables provided in config are valid
//
// If a wildcard table is given, then no other tables can be provided
func isValidTable(tables []string) bool {
	if len(tables) > 1 {
		for _, item := range tables {
			if item == WILDCARD {
				return false
			}
		}
	}

	return true
}

func sourceKey(schema, table string) string {
	return fmt.Sprintf(SOURCE_KEY_FORMAT, schema, table)
}

func initLogger(ctx context.Context, cfg *config.Config) (*log.Logger, error) {
	logger.WithContext(ctx).Info("[SyncManager.initLogger]initializing logger", zap.Uint32("server id", cfg.ServerId))

	// if err := os.MkdirAll("synclog", os.ModePerm); err != nil {
	// 	logger.WithContext(ctx).Error("[SyncManager.initLogger]fail to initialize canal logger dir", zap.Error(err))

	// 	return nil, ErrLogger.New(fmt.Sprintf("[SyncManager.initLogger]%s", err.Error()))
	// }

	logFileName := fmt.Sprintf("canal%d.log", cfg.ServerId)

	_, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, LOG_PERMISSION)
	if err == nil {
		fileHandler, handlerErr := log.NewFileHandler(logFileName, LOG_PERMISSION)
		if handlerErr == nil {
			return log.NewDefault(fileHandler), nil
		}
	}

	logger.WithContext(ctx).Error("[SyncManager.initLogger]fail to initialize canal logger", zap.Error(err))

	return nil, ErrLogger.New(fmt.Sprintf("[SyncManager.initLogger]%s", err.Error()))
}
