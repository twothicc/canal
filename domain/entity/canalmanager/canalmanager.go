package canalmanager

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

type CanalManager interface {
	Run(ctx context.Context, isLegacySync bool) error
	Close(ctx context.Context)
}

type canalManager struct {
	cfg          *config.Config
	eventHandler sync.SyncEventHandler
	canal        *canal.Canal
}

func NewCanalManager(ctx context.Context, cfg *config.Config, eventHandler sync.SyncEventHandler) CanalManager {
	return &canalManager{
		cfg:          cfg,
		eventHandler: eventHandler,
	}
}

func (cm *canalManager) Run(ctx context.Context, isLegacySync bool) error {
	canalCfg := parseCanalCfg(ctx, cm.cfg, isLegacySync)

	newCanal, err := canal.NewCanal(canalCfg)
	if err != nil {
		logger.WithContext(ctx).Error("[CanalManager.Run]fail to initialize canal", zap.Error(err))

		return ErrConfig.New(fmt.Sprintf("[CanalManager.Run]%s", err.Error()))
	}

	cm.canal = newCanal

	cm.canal.SetEventHandler(cm.eventHandler)

	if err = cm.parseSource(ctx, cm.cfg); err != nil {
		logger.WithContext(ctx).Error(
			"[CanalManager.Run]fail to parse source",
			zap.Error(err),
		)

		return err
	}

	return cm.canal.Run()
}

func (cm *canalManager) Close(ctx context.Context) {
	logger.WithContext(ctx).Info("[CanalManager.Close]closing")

	cm.canal.Close()
}

func (cm *canalManager) parseSource(ctx context.Context, cfg *config.Config) error {
	if cm.canal == nil {
		logger.WithContext(ctx).Error("[CanalManager.parseSource]canal not initialized")

		return ErrNoCanal.New("[CanalManager.parseSource]canal not initialized")
	}

	wildCardTables := make(map[string][]string, len(cfg.Sources))

	for _, source := range cfg.Sources {
		if !isValidTable(source.Tables) {
			logger.WithContext(ctx).Error(
				"[CanalManager.parseSource]invalid tables",
				zap.Strings("tables", source.Tables),
			)

			return ErrConfig.New("[CanalManager.parseSource]invalid tables")
		}

		for _, table := range source.Tables {
			if regexp.QuoteMeta(table) != table {
				key := sourceKey(source.Schema, table)

				if _, ok := wildCardTables[key]; ok {
					logger.WithContext(ctx).Error(
						"[CanalManager.parseSource]duplicate wildcard table",
						zap.String("source key", key),
					)

					return ErrConfig.New(fmt.Sprintf("[CanalManager.parseSource]duplicate wildcard table %s", key))
				}

				tableParam := table
				if table == WILDCARD {
					tableParam = ANY_TABLE
				}

				res, err := cm.canal.Execute(WILDCARD_TABLE_SQL, tableParam, source.Schema)
				if err != nil {
					logger.WithContext(ctx).Error(
						"[CanalManager.parseSource]fail to query table info",
						zap.String("raw sql", fmt.Sprintf(WILDCARD_TABLE_SQL, tableParam, source.Schema)),
						zap.Error(err),
					)

					return ErrQuery.New(fmt.Sprintf("[CanalManager.parseSource]%s", err.Error()))
				}

				tables := []string{}

				for rowNum := 0; rowNum < res.Resultset.RowNumber(); rowNum++ {
					tableName, _ := res.GetString(rowNum, 0)
					tables = append(tables, tableName)
				}

				cm.canal.AddDumpTables(source.Schema, tables...)

				wildCardTables[key] = tables
			} else {
				cm.canal.AddDumpTables(source.Schema, table)
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

	// By default, don't log
	nullHandler, _ := log.NewNullHandler()
	canalCfg.Logger = log.NewDefault(nullHandler)

	logFileName := fmt.Sprintf("synclog/canal%d.log", cfg.ServerId)
	_, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		logger.WithContext(ctx).Error("[CanalManager.parseCanalCfg]fail to create canal logger file", zap.Error(err))
	} else {
		fileHandler, err := log.NewFileHandler(logFileName, 0666)
		if err != nil {
			logger.WithContext(ctx).Error("[CanalManager.parseCanalCfg]fail to initialize canal logger", zap.Error(err))
		} else {
			canalCfg.Logger = log.NewDefault(fileHandler)
		}
	}

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

func sourceKey(schema string, table string) string {
	return fmt.Sprintf(SOURCE_KEY_FORMAT, schema, table)
}
