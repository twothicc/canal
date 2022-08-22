package sync

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/twothicc/canal/domain/entity/synccontroller"
	"github.com/twothicc/canal/tools/httpcode"
)

func newStopHandler(ctx context.Context, syncController synccontroller.SyncController) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req StopRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.AbortWithError(httpcode.HTTP_BAD_REQUEST, err)

			return
		}

		if err := syncController.Stop(ctx, req.ServerId); err != nil {
			c.AbortWithError(httpcode.HTTP_BAD_REQUEST, err)

			return
		}

		c.JSON(httpcode.HTTP_OK, StopResponse{
			ServerId: req.ServerId,
			Msg:      fmt.Sprintf("server %d successfully stopped", req.ServerId),
		})
	}
}
