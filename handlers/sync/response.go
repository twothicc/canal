package sync

import "github.com/twothicc/canal/domain/entity/syncmanager"

type RunResponse struct {
	ServerId uint32
	Msg      string
}

type StopResponse struct {
	ServerId uint32
	Msg      string
}

type DeleteResponse struct {
	ServerId uint32
	Msg      string
}

type StatusResponse struct {
	Statuses map[uint32]syncmanager.Status
}
