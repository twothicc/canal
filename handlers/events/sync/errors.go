package sync

import (
	"github.com/twothicc/common-go/errortype"
)

const pkg = "handlers/events/sync"

//nolint:gomnd // error code
var (
	ErrMarshal = errortype.ErrorType{Code: 1, Pkg: pkg}
	ErrEvent   = errortype.ErrorType{Code: 2, Pkg: pkg}
	ErrParse   = errortype.ErrorType{Code: 3, Pkg: pkg}
)
