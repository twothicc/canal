package httprouter

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/twothicc/canal/vendor/github.com/twothicc/common-go/logger"
	"go.uber.org/zap"
)

type errorResponse struct {
	msg string
}

func ErrorHandler(ctx context.Context) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if c.IsAborted() {
			errorMsg := ""

			fields := []zap.Field{}
			for _, err := range c.Errors {
				fields = append(fields, zap.Error(err))

				if errorMsg == "" {
					errorMsg = err.Error()
				} else if errorMsg != "" {
					errorMsg = fmt.Sprintf("%s\n%s", errorMsg, err.Error())
				}
			}

			statusCode := c.Writer.Status()

			if len(fields) != 0 {
				logger.WithContext(ctx).Error("[HttpRouter.ErrorHandler]errors", fields...)
			}

			c.JSON(statusCode, errorResponse{
				msg: errorMsg,
			})
		}
	}
}
