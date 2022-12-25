package kafka

import (
	"github.com/twothicc/common-go/errortype"
)

const pkg = "handlers/events/kafka"

//nolint:gomnd // error code
var (
	ErrConstructor = errortype.ErrorType{Code: 1, Pkg: pkg}
	ErrProduce     = errortype.ErrorType{Code: 1, Pkg: pkg}
)
