package canalmanager

import (
	"context"
	"fmt"
	"regexp"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/twothicc/canal/config"
)

type CanalManager interface {
	PrepareAndRun(ctx context.Context)
}

type canalManager struct {
	canal *canal.Canal
}

func NewCanalManager(_ context.Context, cfg *config.Config) (CanalManager, error) {
	canalCfg := canal.NewDefaultConfig()

	dbCfg := cfg.DbConfig
	canalCfg.Addr = dbCfg.Addr
	canalCfg.User = dbCfg.User
	canalCfg.Password = dbCfg.Pass
	canalCfg.Charset = dbCfg.Charset

	canalCfg.ServerID = cfg.ServerId

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

	newCanal, err := canal.NewCanal(canalCfg)
	if err != nil {
		return nil, ErrConfig.New(fmt.Sprintf("[CanalManager.InitCanal]%s", err.Error()))
	}

	return &canalManager{
		canal: newCanal,
	}, nil
}

func (cm *canalManager) PrepareAndRun(ctx context.Context) {
	return
}

func (cm *canalManager) prepare(ctx context.Context, cfg *config.Config) error {
	wildCardTables := make(map[string][]string, len(cfg.Sources))

	for _, source := range cfg.Sources {
		if !isValidTable(source.Tables) {
			return ErrConfig.New("[CanalManager.prepare]invalid tables")
		}

		for _, table := range source.Tables {
			if regexp.QuoteMeta(table) != table {
				key := sourceKey(source.Schema, table)

				if _, ok := wildCardTables[key]; ok {
					return ErrConfig.New(fmt.Sprintf("[CanalManager.prepare]duplicate wildcard table %s", key))
				}
			}
		}
	}

	return nil
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
	return strings.to
}
