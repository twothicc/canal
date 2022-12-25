package syncmanager

import "time"

// Special characters for parsing / querying tables
const (
	WILDCARD  = "*"
	ANY_TABLE = ".*"
)

// Format specifiers
const (
	SOURCE_KEY_FORMAT  = "%s.%s"
	WILDCARD_TABLE_SQL = `"SELECT table_name FROM information_schema.tables WHERE table_name RLIKE "%s" AND table_schema = "%s";`
)

// Log constants
const (
	LOG_PERMISSION = 0o644
)

// sync constants
const (
	SAVE_INTERVAL     = 3 * time.Second
	SYNC_CHANNEL_SIZE = 4096
)
