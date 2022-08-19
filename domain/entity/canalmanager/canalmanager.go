package canalmanager

import (
	"context"
	"fmt"
	"regexp"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/twothicc/canal/config"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type CanalManager interface {
	Run(ctx context.Context, isLegacySync bool) error
}

type canalManager struct {
	cfg   *config.Config
	canal *canal.Canal
}

func NewCanalManager(_ context.Context, cfg *config.Config) CanalManager {
	return &canalManager{
		cfg: cfg,
	}
}

func (cm *canalManager) Run(ctx context.Context, isLegacySync bool) error {
	canalCfg := parseCanalCfg(cm.cfg, isLegacySync)

	newCanal, err := canal.NewCanal(canalCfg)
	if err != nil {
		logger.WithContext(ctx).Error("[CanalManager.Run]fail to initialize canal", zap.Error(err))

		return ErrConfig.New(fmt.Sprintf("[CanalManager.Run]%s", err.Error()))
	}

	cm.canal = newCanal

	if err = cm.parseSource(ctx, cm.cfg); err != nil {
		logger.WithContext(ctx).Error(
			"[CanalManager.Run]fail to parse source",
			zap.Error(err),
		)

		return err
	}

	return cm.canal.Run()
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

func parseCanalCfg(cfg *config.Config, isLegacySync bool) *canal.Config {
	canalCfg := canal.NewDefaultConfig()

	dbCfg := cfg.DbConfig
	canalCfg.Addr = dbCfg.Addr
	canalCfg.User = dbCfg.User
	canalCfg.Password = dbCfg.Pass
	canalCfg.Charset = dbCfg.Charset

	canalCfg.ServerID = cfg.ServerId

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
