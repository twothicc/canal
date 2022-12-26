package sync

import "time"

const (
	PRIMARY_KEY = "PRIMARY"
	CREATE_TIME = "ctime"
	MODIFY_TIME = "mtime"
)

const (
	INSERT = "insert"
	DELETE = "delete"
	UPDATE = "update"
)

const (
	BASE10 = 10
	BIT32  = 32
)

const (
	RETRY_SECONDS   = 10
	FLUSH_FREQUENCY = 100 * time.Millisecond
)
