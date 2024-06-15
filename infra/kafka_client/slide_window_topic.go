package kafka_client

import (
	"github.com/Shopify/sarama"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/henrion-y/base.services/infra/zlog"
)

type slideWindowTopic struct {
	pool                  *ants.Pool
	handle                func(consumerMessage *sarama.ConsumerMessage) error
	partitionSlideWindows map[int32]*slideWindow
}

func newSlideWindowTopic(maxGoroutine int, handle func(consumerMessage *sarama.ConsumerMessage) error) (*slideWindowTopic, error) {
	t := &slideWindowTopic{
		pool:                  nil,
		handle:                handle,
		partitionSlideWindows: make(map[int32]*slideWindow),
	}
	var err error
	t.pool, err = ants.NewPool(maxGoroutine)
	if err != nil {
		zlog.Error("newSlideWindowTopic.NewPool", zap.Error(err))
	}

	return t, err
}

func (t *slideWindowTopic) setPartitionSlideWindow(partition int32, partitionSlideWindows *slideWindow) {
	t.partitionSlideWindows[partition] = partitionSlideWindows
}

func (t *slideWindowTopic) processMessage(msg *sarama.ConsumerMessage) {
	partitionSlideWindows := t.partitionSlideWindows[msg.Partition]

	partitionSlideWindows.wait(msg.Offset)

	err := t.pool.Submit(func() {
		defer partitionSlideWindows.markOffset(msg.Offset, true)

		err := t.handle(msg)
		if err != nil {
			zlog.Error("slideWindowTopic.processMessage.Handler", zap.Any("kafka_client.msg", msg), zap.Any("t.handle", t.handle), zap.Error(err))
		}
	})
	if err != nil {
		zlog.Error("slideWindowTopic.processMessage.SubmitFunc", zap.Any("kafka_client.msg", msg), zap.Error(err))
	}
}

func (t *slideWindowTopic) release() {
	t.pool.Release()
}
