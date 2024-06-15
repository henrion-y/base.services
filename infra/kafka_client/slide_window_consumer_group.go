package kafka_client

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/henrion-y/base.services/infra/zlog"
)

// NewSlideWindowConsumerGroup 创建滑动窗口模式下并发消费者组
func NewSlideWindowConsumerGroup(addrStr string, groupID string) (*SlideWindowConsumerGroup, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	baseConsumer, err := sarama.NewConsumerGroup(strings.Split(addrStr, ","), groupID, saramaConfig)
	if err != nil {
		zlog.Error("NewSlideWindowConsumerGroup.NewConsumerGroup", zap.Error(err))
		return nil, err
	}

	topicGroup := newSlideWindowTopicGroup()
	consumer := &SlideWindowConsumerGroup{
		topicGroup:    topicGroup,
		consumerGroup: baseConsumer,
	}

	return consumer, nil
}

type SlideWindowConsumerGroup struct {
	consumerGroup sarama.ConsumerGroup
	topicGroup    *slideWindowTopicGroup
}

// SetTopicHandle 设置topic处理程序
func (g *SlideWindowConsumerGroup) SetTopicHandle(topic string, maxGoroutine int, windowSize int, concurrencyModel string, handle func(consumerMessage *sarama.ConsumerMessage) error) {
	if maxGoroutine <= 0 {
		maxGoroutine = 1
	}
	if windowSize <= 0 {
		windowSize = 1
	}
	if maxGoroutine > windowSize {
		maxGoroutine = windowSize
	}

	// windowSize > maxGoroutine 可缓解队头阻塞
	g.topicGroup.setTopicHandle(topic, maxGoroutine, windowSize, concurrencyModel, handle)
}

// Run 运行滑动窗口消费者组
func (g *SlideWindowConsumerGroup) Run() {

	// 初始化一个信号通道，以便在收到SIGINT或SIGTERM时关闭消费者组
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)

	// 创建一个消费者组处理器
	var topics []string
	for topic := range g.topicGroup.topicHandle {
		topics = append(topics, topic)
	}
	// 通过Consume()方法开始消费
	go func() {
		for {
			if err := g.consumerGroup.Consume(context.Background(), topics, g.topicGroup); err != nil {
				log.Fatalln("Error from consumer group:", err)
			}
			// 检查收到的信号，如果是SIGINT或SIGTERM则退出循环
			select {
			case <-g.topicGroup.ready:
			case <-sigterm:
				// 如果收到信号，关闭消费者组并退出
				_ = g.consumerGroup.Close()
			}
		}
	}()

	// 等待程序退出
	<-g.topicGroup.closed
}
