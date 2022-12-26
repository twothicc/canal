package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twothicc/canal/config"
	"github.com/twothicc/common-go/logger"
	"go.uber.org/zap"

	sarama "gopkg.in/Shopify/sarama.v1"
)

type MessageProducer struct {
	sarama.AsyncProducer
	inputCh chan<- *sarama.ProducerMessage
	topic   string
}

type IMessageProducer interface {
	sarama.AsyncProducer
	Produce(ctx context.Context, msg IMessage)
}

func NewMessageProducer(
	ctx context.Context,
	kafkaCfg config.KafkaConfig,
) (IMessageProducer, error) {
	saramaCfg := sarama.NewConfig()

	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true
	saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	saramaCfg.Producer.Retry.Max = int(kafkaCfg.Retry)
	saramaCfg.Producer.Flush.Frequency = time.Duration(kafkaCfg.Flush) * time.Millisecond

	producer, err := sarama.NewAsyncProducer(kafkaCfg.BrokerList, saramaCfg)
	if err != nil {
		logger.WithContext(ctx).Error("[newMessageProducer]Failed to start Sarama producer", zap.Error(err))

		return nil, ErrConstructor.Wrap(err)
	}

	return &MessageProducer{
		AsyncProducer: producer,
		topic:         kafkaCfg.Topic,
	}, nil
}

func (m *MessageProducer) Produce(ctx context.Context, msg IMessage) {
	if m.inputCh == nil {
		go func() {
			for {
				select {
				case successMsg := <-m.Successes():
					logger.WithContext(ctx).Debug(
						fmt.Sprintf("[MessageProducer.Produce]msg stored in topic(%s)/partition(%d)/offset(%d)",
							successMsg.Topic, successMsg.Partition, successMsg.Offset,
						))
				case errorMsg := <-m.Errors():
					logger.WithContext(ctx).Error(
						"[MessageProducer.Produce]failed to produce message",
						zap.Error(errorMsg.Err),
					)
				}
			}
		}()

		m.inputCh = m.Input()
	}

	producerMessage := &sarama.ProducerMessage{
		Topic: m.topic,
		Key:   sarama.StringEncoder(fmt.Sprintf(syncMsgFormat, m.topic, msg.Key())),
		Value: msg,
	}

	m.inputCh <- producerMessage
}
