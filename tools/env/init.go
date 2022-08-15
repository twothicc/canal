package env

import (
	"context"
	"os"

	"github.com/joho/godotenv"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type envConfigs struct {
	ServiceName string
	Domain      string
	Port        string
	Env         string
}

var EnvConfigs = &envConfigs{}

func Init(ctx context.Context) {
	err := godotenv.Load()
	if err != nil {
		logger.WithContext(ctx).Error("fail to init env", zap.Error(err))
	}

	EnvConfigs.ServiceName = os.Getenv(SERVICE_NAME)
	EnvConfigs.Domain = os.Getenv(DOMAIN)
	EnvConfigs.Port = os.Getenv(PORT)
	EnvConfigs.Env = os.Getenv(ENV)
}

// IsTest - Indicates if environment is test or production
func IsTest() bool {
	return EnvConfigs.Env == TEST
}
