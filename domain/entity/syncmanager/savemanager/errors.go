package savemanager

import "github.com/twothicc/common-go/errortype"

const pkg = "domain/entity/syncmanager/savemanager"

//nolint:gomnd // error code
var (
	ErrFile = errortype.ErrorType{Code: 1, Pkg: pkg}
)
