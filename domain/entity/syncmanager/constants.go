package syncmanager

const (
	WILDCARD  = "*"
	ANY_TABLE = ".*"
)

const (
	SOURCE_KEY_FORMAT  = "%s.%s"
	WILDCARD_TABLE_SQL = `"SELECT table_name FROM information_schema.tables WHERE table_name RLIKE "%s" AND table_schema = "%s";`
)

const (
	LOG_PERMISSION = 0o644
)
