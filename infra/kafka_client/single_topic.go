package kafka_client

import (
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/henrion-y/base.services/infra/zlog"
)

func newSingleTopic(handle func(consumerMessage *sarama.ConsumerMessage) error) (*singleTopic, error) {
	t := &singleTopic{
		handle:                handle,
		partitionSlideWindows: make(map[int32]*slideWindow),
	}

	return t, nil
}

type singleTopic struct {
	partitionSlideWindows map[int32]*slideWindow
	handle                func(consumerMessage *sarama.ConsumerMessage) error
}

func (t *singleTopic) setPartitionSlideWindow(partition int32, partitionSlideWindows *slideWindow) {
	t.partitionSlideWindows[partition] = partitionSlideWindows
}

func (t *singleTopic) processMessage(msg *sarama.ConsumerMessage) {
	defer func() {
		if r := recover(); r != nil {
			zlog.Error("slideWindowTopic.processMessage.panic", zap.Any("recover", r))
			time.Sleep(2 * time.Second)
		}
	}()

	err := t.handle(msg)
	if err != nil {
		zlog.Error("slideWindowTopic.singleProcessMessage", zap.Any("kafka_client.msg", msg), zap.Any("t.handle", t.handle), zap.Error(err))
	}
	t.partitionSlideWindows[msg.Partition].sess.MarkMessage(msg, "")
}

func (t *singleTopic) release() {

}
