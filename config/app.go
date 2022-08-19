package config

import (
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

type DbConfig struct {
	DbAddr    string `toml:"addr"`
	DbUser    string `toml:"user"`
	DbPass    string `toml:"pass"`
	DbCharset string `toml:"charset"`
	DbFlavor  string `toml:"flavor"`
}

type Config struct {
	DbConfig DbConfig `toml:"database"`

	ServerId     uint32 `toml:"server_id"`
	DumpExecPath string `toml:"dump.mysqldump_path"`

	Sources []SourceConfig `toml:"source"`
}

func NewConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, ErrNotFound.New(fmt.Sprintf("[NewConfig]%s", err.Error()))
	}

	var c Config

	_, err = toml.Decode(string(data), &c)
	if err != nil {
		return nil, ErrParse.New(fmt.Sprintf("[NewConfig]%s", err.Error()))
	}

	return &c, nil
}
