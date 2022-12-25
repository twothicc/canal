package sync

import "github.com/twothicc/canal/config"

type RunRequest struct {
	Addr    string
	User    string
	Pass    string
	Charset string
	flavor  string
	Kafka   config.KafkaConfig
	Sources []config.SourceConfig
}

type StopRequest struct {
	ServerId uint32
}

type DeleteRequest struct {
	ServerId uint32
}
