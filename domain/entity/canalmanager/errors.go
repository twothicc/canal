package canalmanager

import (
	"github.com/twothicc/common-go/errortype"
)

const pkg = "domain/entity/canalmanager"

var (
	ErrConfig = errortype.ErrorType{Code: 1, Pkg: pkg}
)
