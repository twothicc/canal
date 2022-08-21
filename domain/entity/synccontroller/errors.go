package synccontroller

import (
	"github.com/twothicc/common-go/errortype"
)

const pkg = "domain/entity/synccontroller"

//nolint:gomnd // error code
var (
	ErrParam = errortype.ErrorType{Code: 1, Pkg: pkg}
)
