package syncmanager

import (
	"context"
	"fmt"
	"os"

	"regexp"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/siddontang/go-log/log"
	"github.com/twothicc/canal/config"
	"github.com/twothicc/canal/handlers/events/sync"
	"github.com/twothicc/canal/tools/env"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

// SyncManager - manages data sync
type SyncManager interface {
	Run(ctx context.Context, isLegacySync bool) error
	Close(ctx context.Context)
}

type syncManager struct {
	cfg          *config.Config
	eventHandler sync.SyncEventHandler
	canal        *canal.Canal
}

// NewSyncManager - creates a SyncManager
func NewSyncManager(ctx context.Context, cfg *config.Config, eventHandler sync.SyncEventHandler) SyncManager {
	return &syncManager{
		cfg:          cfg,
		eventHandler: eventHandler,
	}
}

// Run - starts data sync
//
// isLegacySync indicates whether the sync will include existing records
func (sm *syncManager) Run(ctx context.Context, isLegacySync bool) error {
	canalCfg := parseCanalCfg(ctx, sm.cfg, isLegacySync)

	newCanal, err := canal.NewCanal(canalCfg)
	if err != nil {
		logger.WithContext(ctx).Error("[SyncManager.Run]fail to initialize canal", zap.Error(err))

		return ErrConfig.New(fmt.Sprintf("[SyncManager.Run]%s", err.Error()))
	}

	sm.canal = newCanal

	sm.canal.SetEventHandler(sm.eventHandler)

	if err = sm.parseSource(ctx, sm.cfg); err != nil {
		logger.WithContext(ctx).Error(
			"[SyncManager.Run]fail to parse source",
			zap.Error(err),
		)

		return err
	}

	return sm.canal.Run()
}

// Close - closes underlying canal, stopping data sync immediately
func (sm *syncManager) Close(ctx context.Context) {
	logger.WithContext(ctx).Info("[SyncManager.Close]closing")

	sm.canal.Close()
}

// parseSource - parses special characters in tables from config source into valid tables
func (sm *syncManager) parseSource(ctx context.Context, cfg *config.Config) error {
	if sm.canal == nil {
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

				res, err := sm.canal.Execute(WILDCARD_TABLE_SQL, tableParam, source.Schema)
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

				sm.canal.AddDumpTables(source.Schema, tables...)

				wildCardTables[key] = tables
			} else {
				sm.canal.AddDumpTables(source.Schema, table)
			}
		}
	}

	return nil
}

func parseCanalCfg(ctx context.Context, cfg *config.Config, isLegacySync bool) *canal.Config {
	canalCfg := canal.NewDefaultConfig()

	dbCfg := cfg.DbConfig
	canalCfg.Addr = dbCfg.Addr
	canalCfg.User = dbCfg.User
	canalCfg.Password = env.EnvConfigs.DbPass
	canalCfg.Charset = dbCfg.Charset

	canalCfg.ServerID = cfg.ServerId

	canalLogger, err := initLogger(ctx, cfg)
	if err != nil {
		nullHandler, _ := log.NewNullHandler()
		canalLogger = log.NewDefault(nullHandler)
	}

	canalCfg.Logger = canalLogger

	if isLegacySync {
		dumpCfg := cfg.DumpConfig
		canalCfg.Dump.ExecutionPath = dumpCfg.DumpExecPath
	}

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
	path := fmt.Sprintf("synclog/canal%d.log", cfg.ServerId)

	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, ErrLogger.New(fmt.Sprintf("[SyncManager.initLogger]%s", err.Error()))
	}

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
