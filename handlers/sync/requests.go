package sync

import "github.com/twothicc/canal/config"

type RunRequest struct {
	Addr    string
	User    string
	Pass    string
	Charset string
	flavor  string
	Sources []config.SourceConfig
	Kafka   config.KafkaConfig
}

type StopRequest struct {
	ServerId uint32
}

type DeleteRequest struct {
	ServerId uint32
}
