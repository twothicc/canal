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

func NewDeleteHandler(ctx context.Context, syncController synccontroller.SyncController) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req DeleteRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			if abortErr := c.AbortWithError(httpcode.HTTP_BAD_REQUEST, err); abortErr != nil {
				logger.WithContext(ctx).Error("[NewDeleteHandler]fail to abort after failed JSON bind", zap.Error(err))
			}

			return
		}

		if err := syncController.Remove(ctx, req.ServerId); err != nil {
			if abortErr := c.AbortWithError(httpcode.HTTP_BAD_REQUEST, err); abortErr != nil {
				logger.WithContext(ctx).Error(
					"[NewDeleteHandler]fail to abort after failed syncmanager removal",
					zap.Error(err),
					zap.Uint32("server id", req.ServerId),
				)
			}

			return
		}

		c.JSON(httpcode.HTTP_OK, DeleteResponse{
			ServerId: req.ServerId,
			Msg:      fmt.Sprintf("server %d successfully deleted", req.ServerId),
		})
	}
}
