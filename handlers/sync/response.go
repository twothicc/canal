package sync

import "github.com/twothicc/canal/domain/entity/syncmanager"

type RunResponse struct {
	Msg      string
	ServerId uint32
}

type StopResponse struct {
	Msg      string
	ServerId uint32
}

type DeleteResponse struct {
	Msg      string
	ServerId uint32
}

type StatusResponse struct {
	Statuses map[uint32]syncmanager.Status
}
