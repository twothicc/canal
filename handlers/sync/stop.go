package sync

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/twothicc/canal/domain/entity/synccontroller"
	"github.com/twothicc/canal/tools/httpcode"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

func NewStopHandler(ctx context.Context, syncController synccontroller.SyncController) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req StopRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			if abortErr := c.AbortWithError(httpcode.HTTP_BAD_REQUEST, err); abortErr != nil {
				logger.WithContext(ctx).Error("[NewStopHandler]fail to abort after failed JSON bind", zap.Error(err))
			}

			return
		}

		if err := syncController.Stop(ctx, req.ServerId); err != nil {
			if abortErr := c.AbortWithError(httpcode.HTTP_BAD_REQUEST, err); abortErr != nil {
				logger.WithContext(ctx).Error(
					"[NewStopHandler]fail to abort after failed syncmanager stop",
					zap.Error(err),
					zap.Uint32("server id", req.ServerId),
				)
			}

			return
		}

		c.JSON(httpcode.HTTP_OK, StopResponse{
			ServerId: req.ServerId,
			Msg:      fmt.Sprintf("server %d successfully stopped", req.ServerId),
		})
	}
}
