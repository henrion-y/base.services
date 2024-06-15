package kafka_client

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"

	"github.com/henrion-y/base.services/infra/zlog"
)

func newSlideWindowPartition(maxGoroutine int, handle func(consumerMessage *sarama.ConsumerMessage) error) (*slideWindowPartition, error) {
	t := &slideWindowPartition{
		close:                 make(chan struct{}),
		partitionChan:         make([]chan *sarama.ConsumerMessage, maxGoroutine),
		handle:                handle,
		partitionSlideWindows: make(map[int32]*slideWindow),
	}

	for i := range t.partitionChan {
		t.partitionChan[i] = make(chan *sarama.ConsumerMessage)
		go t.processMessageByRouter(t.partitionChan[i])
	}

	return t, nil
}

type slideWindowPartition struct {
	close                 chan struct{}
	handle                func(consumerMessage *sarama.ConsumerMessage) error
	partitionSlideWindows map[int32]*slideWindow
	partitionChan         []chan *sarama.ConsumerMessage
}

func (t *slideWindowPartition) setPartitionSlideWindow(partition int32, partitionSlideWindows *slideWindow) {
	t.partitionSlideWindows[partition] = partitionSlideWindows
}

func (t *slideWindowPartition) processMessageByRouter(msg <-chan *sarama.ConsumerMessage) {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					zlog.Error("slideWindowTopic.processMessage.panic", zap.Any("recover", r))
					time.Sleep(2 * time.Second)
				}
			}()

			for {
				select {
				case <-t.close:
					return
				case partitionMsg, ok := <-msg:
					if !ok {
						zlog.Warn("slideWindowPartition.processMessageByRouter.channel.closed")
						return
					}

					partitionSlideWindows, ok := t.partitionSlideWindows[partitionMsg.Partition]
					if !ok {
						zlog.Error("slideWindowPartition.processMessageByRouter.partition.not.found", zap.Int32("partition", partitionMsg.Partition))
						continue
					}
					// 添加异常恢复
					err := t.handle(partitionMsg)
					if err != nil {
						zlog.Error("slideWindowPartition.processMessageByRouter.Handler", zap.Any("kafka_client.msg", msg), zap.Any("t.handle", t.handle), zap.Error(err))
					}

					partitionSlideWindows.markOffset(partitionMsg.Offset, true)
				}
			}
		}()
	}
}

func (t *slideWindowPartition) processMessage(msg *sarama.ConsumerMessage) {

	partitionSlideWindows := t.partitionSlideWindows[msg.Partition]

	partitionSlideWindows.wait(msg.Offset)

	route := hashMod(string(msg.Key), len(t.partitionChan))

	t.partitionChan[route] <- msg
}

func (t *slideWindowPartition) release() {
	close(t.close)
}

// hashMod 字符串取模做hash路由
func hashMod(str string, mod int) int {
	hash := xxhash.Sum64String(str)
	return int(hash % uint64(mod))
}
