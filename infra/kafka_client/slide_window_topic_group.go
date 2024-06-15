package kafka_client

import (
	"github.com/Shopify/sarama"
)

// CONSUMERMODEL_SERIALIZATION_BY_KEY 在消费时按key将消息下发给对应对协程，适合需要根据key保证操作顺序的场景
const CONSUMERMODEL_SERIALIZATION_BY_KEY = "SerializationByKey"

type topicSlideWindow interface {
	release()
	processMessage(msg *sarama.ConsumerMessage)
	setPartitionSlideWindow(partition int32, partitionSlideWindows *slideWindow)
}

func newSlideWindowTopicGroup() *slideWindowTopicGroup {
	return &slideWindowTopicGroup{
		sess:               nil,
		ready:              make(chan bool),
		closed:             make(chan bool),
		topicWindowSize:    make(map[string]int),
		topicMaxGoroutine:  make(map[string]int),
		topicConsumerModel: make(map[string]string),
		slideWindowTopics:  make(map[string]topicSlideWindow),
		topicHandle:        make(map[string]func(consumerMessage *sarama.ConsumerMessage) error),
	}
}

type slideWindowTopicGroup struct {
	ready              chan bool
	closed             chan bool
	topicWindowSize    map[string]int
	topicMaxGoroutine  map[string]int
	topicConsumerModel map[string]string // 并发模型
	sess               sarama.ConsumerGroupSession
	slideWindowTopics  map[string]topicSlideWindow
	topicHandle        map[string]func(consumerMessage *sarama.ConsumerMessage) error
}

func (g *slideWindowTopicGroup) setTopicHandle(topic string, maxGoroutine int, windowSize int, concurrencyModel string, handle func(consumerMessage *sarama.ConsumerMessage) error) {
	g.topicMaxGoroutine[topic] = maxGoroutine
	g.topicWindowSize[topic] = windowSize
	g.topicHandle[topic] = handle
	g.topicConsumerModel[topic] = concurrencyModel
}

// 初始化
func (g *slideWindowTopicGroup) initLeftOffset(claim sarama.ConsumerGroupClaim) error {
	partitionSlideWindow := newSlideWindow(claim.Topic(), claim.Partition(), g.topicWindowSize[claim.Topic()], claim.InitialOffset(), g.sess)
	var err error
	if g.slideWindowTopics[claim.Topic()] == nil {
		if g.topicMaxGoroutine[claim.Topic()] == 1 || partitionSlideWindow.windowSize == 1 {
			g.slideWindowTopics[claim.Topic()], err = newSingleTopic(g.topicHandle[claim.Topic()])
		} else {
			switch g.topicConsumerModel[claim.Topic()] {
			case CONSUMERMODEL_SERIALIZATION_BY_KEY:
				g.slideWindowTopics[claim.Topic()], err = newSlideWindowPartition(g.topicMaxGoroutine[claim.Topic()], g.topicHandle[claim.Topic()])
			default:
				g.slideWindowTopics[claim.Topic()], err = newSlideWindowTopic(g.topicMaxGoroutine[claim.Topic()], g.topicHandle[claim.Topic()])
			}
		}

		if err != nil {
			return err
		}
	}

	g.slideWindowTopics[claim.Topic()].setPartitionSlideWindow(claim.Partition(), partitionSlideWindow)

	return nil
}

func (g *slideWindowTopicGroup) Setup(sess sarama.ConsumerGroupSession) error {
	g.sess = sess
	//close(g.ready) // 当消费者组准备好时关闭通道
	return nil
}

func (g *slideWindowTopicGroup) Cleanup(_ sarama.ConsumerGroupSession) error {
	for _, group := range g.slideWindowTopics {
		group.release() // 关闭 Goroutine 池
	}
	//close(g.closed) // 当消费者组关闭时关闭通道
	return nil
}

func (g *slideWindowTopicGroup) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	err := g.initLeftOffset(claim)
	if err != nil {
		return err
	}

	for message := range claim.Messages() {
		g.slideWindowTopics[message.Topic].processMessage(message)
	}

	return nil
}
