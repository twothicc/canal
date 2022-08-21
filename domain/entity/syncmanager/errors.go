package syncmanager

import (
	"github.com/twothicc/common-go/errortype"
)

const pkg = "domain/entity/syncmanager"

//nolint:gomnd // error code
var (
	ErrConfig  = errortype.ErrorType{Code: 1, Pkg: pkg}
	ErrQuery   = errortype.ErrorType{Code: 2, Pkg: pkg}
	ErrNoCanal = errortype.ErrorType{Code: 3, Pkg: pkg}
	ErrLogger  = errortype.ErrorType{Code: 4, Pkg: pkg}
	ErrBinlog  = errortype.ErrorType{Code: 5, Pkg: pkg}
	ErrParam   = errortype.ErrorType{Code: 6, Pkg: pkg}
	ErrSave    = errortype.ErrorType{Code: 7, Pkg: pkg}
)
