package kafka

import (
	"context"
	"fmt"

	"github.com/twothicc/canal/config"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"

	sarama "gopkg.in/Shopify/sarama.v1"
)

type MessageProducer struct {
	sarama.SyncProducer
	topic string
}

type IMessageProducer interface {
	sarama.SyncProducer
	Produce(ctx context.Context, msg IMessage) error
}

func NewMessageProducer(
	ctx context.Context,
	kafkaCfg config.KafkaConfig,
) (IMessageProducer, error) {
	saramaCfg := sarama.NewConfig()

	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 10

	producer, err := sarama.NewSyncProducer(kafkaCfg.BrokerList, saramaCfg)
	if err != nil {
		logger.WithContext(ctx).Error("[newMessageProducer]Failed to start Sarama producer", zap.Error(err))

		return nil, ErrConstructor.Wrap(err)
	}

	return &MessageProducer{
		SyncProducer: producer,
		topic:        kafkaCfg.Topic,
	}, nil
}

func (m *MessageProducer) Produce(ctx context.Context, msg IMessage) error {
	producerMessage := &sarama.ProducerMessage{
		Topic: m.topic,
		Key:   sarama.StringEncoder(fmt.Sprintf(syncMsgFormat, m.topic, msg.Key())),
		Value: msg,
	}

	partition, offset, err := m.SendMessage(producerMessage)
	if err != nil {
		return ErrProduce.Wrap(err)
	}

	logger.WithContext(ctx).Debug("[MessageProducer.Produce]" + fmt.Sprintf("msg stored in topic(%s)/partition(%d)/offset(%d)", m.topic, partition, offset))

	return nil
}
