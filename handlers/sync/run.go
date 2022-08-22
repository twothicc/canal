package sync

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/twothicc/canal/config"
	"github.com/twothicc/canal/domain/entity/synccontroller"
	"github.com/twothicc/canal/domain/entity/syncmanager"
	"github.com/twothicc/canal/tools/httpcode"

	"github.com/twothicc/common-go/grpcclient"
)

func NewRunHandler(
	ctx context.Context,
	cfg *config.Config,
	client *grpcclient.Client,
	syncController synccontroller.SyncController,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req RunRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.AbortWithError(httpcode.HTTP_BAD_REQUEST, err)
		}

		cfg.DbConfig.Addr = req.Addr
		cfg.DbConfig.User = req.User
		cfg.DbConfig.Pass = req.Pass
		cfg.DbConfig.Charset = req.Charset
		cfg.DbConfig.Flavor = req.flavor

		cfg.Sources = req.Sources

		syncManager, err := syncmanager.NewSyncManager(
			ctx,
			cfg,
			client,
		)
		if err != nil {
			c.AbortWithError(httpcode.HTTP_INTERNAL_SERVER_ERROR, err)

			return
		}

		syncController.Add(ctx, syncManager.GetId(), syncManager)

		if err := syncController.Start(ctx, syncManager.GetId(), false); err != nil {
			c.AbortWithError(httpcode.HTTP_INTERNAL_SERVER_ERROR, err)

			return
		}

		c.JSON(httpcode.HTTP_OK, RunResponse{
			ServerId: syncManager.GetId(),
			Msg:      fmt.Sprintf("successfully started server %d", syncManager.GetId()),
		})
	}
}
