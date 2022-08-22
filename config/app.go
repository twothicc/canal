package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

type DbConfig struct {
	Addr    string `toml:"addr"`
	User    string `toml:"user"`
	Pass    string
	Charset string `toml:"charset"`
	Flavor  string `toml:"flavor"`
}

type DumpConfig struct {
	DumpExecPath string `toml:"mysqldump_path"`
}

type Config struct {
	DbConfig   DbConfig       `toml:"database"`
	DumpConfig DumpConfig     `toml:"dump"`
	Sources    []SourceConfig `toml:"source"`
	ServerId   uint32
}

func NewConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
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
