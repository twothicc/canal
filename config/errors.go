package config

import (
	"github.com/twothicc/common-go/errortype"
)

const pkg = "config"

//nolint:gomnd // error code
var (
	ErrParse    = errortype.ErrorType{Code: 1, Pkg: pkg}
	ErrNotFound = errortype.ErrorType{Code: 2, Pkg: pkg}
)
