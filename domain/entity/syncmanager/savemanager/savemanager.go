package savemanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type SaveInfo struct {
	lastSaveTime time.Time
	Name         string `toml:"bin_name"`
	filePath     string
	mu           sync.RWMutex
	Pos          uint32 `toml:"bin_pos"`
}

type ISaveInfo interface {
	Save(ctx context.Context, pos mysql.Position) error
	Position() mysql.Position
	Close(ctx context.Context) error
}

func LoadSaveInfo(ctx context.Context, serverId uint32) (ISaveInfo, error) {
	var s SaveInfo

	dir := path.Join(SAVE_DIR, strconv.FormatUint(uint64(serverId), BASE10))
	filePath := path.Join(dir, "save.info")

	s.filePath = filePath
	s.lastSaveTime = time.Now()

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		logger.WithContext(ctx).Error("[SaveManager.LoadSaveInfo]fail to create/find dir", zap.Error(err))

		return nil, ErrFile.New(fmt.Sprintf("[SaveManager.LoadSaveInfo]%s", err.Error()))
	}

	file, err := os.Open(filePath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, ErrFile.New(fmt.Sprintf("[SaveManager.LoadSaveInfo]%s", err.Error()))
	} else if errors.Is(err, fs.ErrNotExist) {
		return &s, nil
	}

	defer file.Close()

	_, err = toml.NewDecoder(file).Decode(&s)

	return &s, err
}

func (s *SaveInfo) Save(ctx context.Context, pos mysql.Position) error {
	logger.WithContext(ctx).Info(fmt.Sprintf("[SaveManager.Save]%s", pos))

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Name = pos.Name
	s.Pos = pos.Pos

	if s.filePath == "" {
		return nil
	}

	n := time.Now()
	if n.Sub(s.lastSaveTime) < time.Second {
		return nil
	}

	s.lastSaveTime = n

	var buf bytes.Buffer

	e := toml.NewEncoder(&buf)

	_ = e.Encode(s)

	if err := ioutil2.WriteFileAtomic(s.filePath, buf.Bytes(), SAVE_FILE_PERMISSION); err != nil {
		logger.WithContext(ctx).Error("[SaveManager.Save]fail to write save info", zap.Error(err))

		return ErrFile.New(fmt.Sprintf("[SaveManager.Save]%s", err.Error()))
	}

	return nil
}

func (s *SaveInfo) Position() mysql.Position {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return mysql.Position{
		Name: s.Name,
		Pos:  s.Pos,
	}
}

func (s *SaveInfo) Close(ctx context.Context) error {
	pos := s.Position()

	return s.Save(ctx, pos)
}
