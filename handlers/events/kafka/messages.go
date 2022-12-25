package kafka

import (
	"encoding/json"
	"strings"

	"gopkg.in/Shopify/sarama.v1"
)

const (
	keySeparator = "-"
)

type IMessage interface {
	sarama.Encoder
	Key() string
}

type SyncMessage struct {
	err        error
	Action     string   `json:"action"`
	Table      string   `json:"table"`
	Schema     string   `json:"schema"`
	OldData    []byte   `json:"old_data"`
	NewData    []byte   `json:"new_data"`
	Pk         []string `json:"pk"`
	encoded    []byte
	Ctimestamp uint32 `json:"ctimestamp"`
	Mtimestamp uint32 `json:"mtimestamp"`
}

func (sm *SyncMessage) Key() string {
	return strings.Join(sm.Pk, keySeparator)
}

func (sm *SyncMessage) Length() int {
	sm.ensureEncoded()

	return len(sm.encoded)
}

func (sm *SyncMessage) Encode() ([]byte, error) {
	sm.ensureEncoded()

	return sm.encoded, sm.err
}

func (sm *SyncMessage) ensureEncoded() {
	if sm.encoded == nil && sm.err == nil {
		sm.encoded, sm.err = json.Marshal(sm)
	}
}
