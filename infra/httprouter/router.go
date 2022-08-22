package httprouter

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/twothicc/canal/config"
	"github.com/twothicc/canal/domain/entity/synccontroller"
	"github.com/twothicc/canal/handlers/sync"
	"github.com/twothicc/common-go/grpcclient"
)

type HttpRouterDependencies struct {
	GrpcClient     *grpcclient.Client
	Cfg            *config.Config
	SyncController synccontroller.SyncController
}

func NewHTTPRouter(ctx context.Context, dependencies *HttpRouterDependencies) *gin.Engine {
	router := gin.Default()

	router.Use(ErrorHandler(ctx))

	syncGroup := router.Group("/sync")
	{
		syncGroup.POST("/run", sync.NewRunHandler(
			ctx,
			dependencies.Cfg,
			dependencies.GrpcClient,
			dependencies.SyncController,
		))
	}

	return router
}
