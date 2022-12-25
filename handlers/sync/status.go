package sync

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/twothicc/canal/domain/entity/synccontroller"
	"github.com/twothicc/canal/domain/entity/syncmanager"
	"github.com/twothicc/canal/tools/httpcode"
)

func NewStatusHandler(ctx context.Context, syncController synccontroller.SyncController) gin.HandlerFunc {
	return func(c *gin.Context) {
		respData := make(map[uint32]syncmanager.Status)

		statusMap := syncController.Status()
		for key, val := range statusMap {
			respData[key] = *val
		}

		c.JSON(httpcode.HTTP_OK, StatusResponse{
			Statuses: respData,
		})
	}
}
