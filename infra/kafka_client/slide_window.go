package kafka_client

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// 帮我增强程序的健壮性，
func newSlideWindow(topic string, partition int32, windowSize int, initialOffset int64, sess sarama.ConsumerGroupSession) *slideWindow {
	s := &slideWindow{
		waitNotify:   make(chan struct{}),
		topic:        topic,
		partition:    partition,
		windowSize:   windowSize,
		leftOffset:   initialOffset - 1,
		ackOffset:    make(map[int64]bool),
		mu:           &sync.Mutex{},
		sess:         sess,
		offsetNotify: make(chan int64),
	}
	go s.batchOffset(s.offsetNotify)
	return s
}

type slideWindow struct {
	isWait       bool
	topic        string
	partition    int32
	windowSize   int
	leftOffset   int64
	rightOffset  int64
	offsetNotify chan int64
	waitNotify   chan struct{}
	ackOffset    map[int64]bool
	mu           *sync.Mutex
	sess         sarama.ConsumerGroupSession
}

func (s *slideWindow) batchOffset(offset <-chan int64) {
	// 游标
	cursor := 0
	var maxOffset int64
	timeout := time.NewTimer(time.Second)
	for {
		select {
		case markOffset, ok := <-offset:
			if !ok {
				if cursor > 0 {
					s.sess.MarkOffset(s.topic, s.partition, maxOffset, "")
					return
				}
			}
			maxOffset = markOffset
			cursor++
			if cursor == 10 {
				s.sess.MarkOffset(s.topic, s.partition, maxOffset, "")
				cursor = 0
			}
		case <-timeout.C:
			if cursor > 0 {
				s.sess.MarkOffset(s.topic, s.partition, maxOffset, "")
				cursor = 0
			}
			timeout.Reset(time.Second)
		}
	}
}

func (s *slideWindow) syncOffset(offset int64) {
	s.mu.Lock()
	for i := offset; i <= s.rightOffset+1; i++ {
		if _, ok := s.ackOffset[i]; ok {
			delete(s.ackOffset, i)
		} else {
			s.leftOffset = i - 1
			break
		}
	}
	s.mu.Unlock()

	s.offsetNotify <- s.leftOffset + 1
	if s.isWait {
		<-s.waitNotify
		s.isWait = false
	}
}

func (s *slideWindow) markOffset(offset int64, result bool) {
	s.mu.Lock()
	s.ackOffset[offset] = result
	s.mu.Unlock()

	if s.rightOffset == 0 || offset > s.rightOffset {
		// 更新最大偏移量
		s.rightOffset = offset
	}
	if offset != s.leftOffset+1 {
		// 无需触发窗口滑动
		return
	}
	s.syncOffset(offset)
}

func (s *slideWindow) wait(offset int64) {
	if offset == s.leftOffset+int64(s.windowSize)+1 {
		s.isWait = true
		s.waitNotify <- struct{}{}
	}
}
