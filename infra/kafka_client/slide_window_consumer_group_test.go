package kafka_client

import (
	"fmt"
	"github.com/Shopify/sarama"
	json "github.com/json-iterator/go"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestRunSlideWindowConsumerGroup(t *testing.T) {
	addrStr := "kafka1.aicoin.local:9091,kafka2.aicoin.local:9092,kafka3.aicoin.local:9093"
	groupID := "xxx"
	slideWindowConsumerGroup, _ := NewSlideWindowConsumerGroup(addrStr, groupID)
	slideWindowConsumerGroup.SetTopicHandle("test_slideWindow_partition", 8, 10, CONSUMERMODEL_SERIALIZATION_BY_KEY, trade)
	slideWindowConsumerGroup.SetTopicHandle("test_slideWindow_partition_chat", 3, 5, CONSUMERMODEL_SERIALIZATION_BY_KEY, chat)

	go slideWindowConsumerGroup.Run()
	select {}
}

func CreateProducer() sarama.SyncProducer {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{
		"kafka1.aicoin.local:9091",
		"kafka2.aicoin.local:9092",
		"kafka3.aicoin.local:9093",
	}, saramaConfig)
	if err != nil {
		log.Panicf("Error creating producer: %v", err)
	}

	return producer
}

type userAction struct {
	Uid          int    `json:"uid"`            // 用户id
	DbKey        string `json:"db_key"`         // 交易对主键
	Amount       int64  `json:"amount"`         // 交易金额
	CopyActionID int    `json:"copy_action_id"` // 本次操作id
	Action       string `json:"action"`         // 买卖行为，如 "买入"， "卖出" 等
}

func generationUserAction() []userAction {
	uids := []int{100, 200, 300, 400, 500, 600}
	actions := []string{}
	dbKeys := []string{}

	var userActions []userAction

	rand.Seed(time.Now().UnixNano())

	numActions := 200
	for i := 0; i < numActions; i++ {
		uid := uids[rand.Intn(len(uids))]          // 随机选择用户ID
		action := actions[rand.Intn(len(actions))] // 随机选择操作行为
		amount := rand.Int63n(101)                 // 随机生成交易金额，范围为 0-100
		dbKey := dbKeys[rand.Intn(len(dbKeys))]    // 随机选择交易对主键

		ua := userAction{
			Uid:          uid,
			DbKey:        dbKey,
			Amount:       amount,
			CopyActionID: i + 1,
			Action:       action,
		}
		userActions = append(userActions, ua)
	}

	return userActions
}

func trade(consumerMessage *sarama.ConsumerMessage) error {
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
	randomDuration := time.Duration(rand.Intn(3000)) * time.Millisecond
	time.Sleep(randomDuration)

	action := userAction{}
	err := json.Unmarshal(consumerMessage.Value, &action)
	if err != nil {
		fmt.Println("trade.Unmarshal", err)
		return err
	}

	return nil
}

func TestRunSlideWindowConsumerGroup_SendTrade(t *testing.T) {
	producer := CreateProducer()

	defer func() {
		if err := producer.Close(); err != nil {
			log.Panicf("Error closing producer: %v", err)
		}
	}()

	topic := "test_slideWindow_partition"
	actions := generationUserAction()
	for i := range actions {
		value, _ := json.Marshal(actions[i])
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
			Key:   sarama.StringEncoder(fmt.Sprintf("%d", actions[i].Uid)),
		}
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			fmt.Printf("Failed to send message: %v", err)
		} else {
			fmt.Printf("交易创建成功： partition = %d, offset = %d, actions = %s \n", partition, offset, string(value))
		}
	}

}

func TestNewSlideWindowConsumerGroup(t *testing.T) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Return.Successes = true

	admin, err := sarama.NewClusterAdmin([]string{
		"kafka1.aicoin.local:9091",
		"kafka2.aicoin.local:9092",
		"kafka3.aicoin.local:9093",
	}, saramaConfig)
	if err != nil {
		log.Panicf("Error creating producer: %v", err)
	}

	defer func() {
		if err := admin.Close(); err != nil {
			log.Panicf("Error closing producer: %v", err)
		}
	}()

	topicName := "test_slideWindow_partition_chat"

	numPartitions := 5

	// 创建 topic
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(numPartitions),
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		log.Fatal("Failed to create topic:", err)
	}
}

type chatMsg struct {
	Uid       int       `json:"uid"`        // 用户id
	SessionId string    `json:"session_id"` // 会话id
	MsgId     int       `json:"msg_id"`     // 消息id
	Payload   string    `json:"payload"`    // 消息负载
	CreateAt  time.Time `json:"create_at"`  // 消息发送时间
}

func chat(consumerMessage *sarama.ConsumerMessage) error {
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
	randomDuration := time.Duration(rand.Intn(3000)) * time.Millisecond
	time.Sleep(randomDuration)

	msg := chatMsg{}
	err := json.Unmarshal(consumerMessage.Value, &msg)
	if err != nil {
		fmt.Println("trade.Unmarshal", err)
		return err
	}

	redisDataJSON, _ := json.Marshal(msg)
	fmt.Println("处理聊天消息：partition = ", consumerMessage.Partition, "offset = ", consumerMessage.Offset, "  data = ", string(redisDataJSON))
	return nil
}

func generationChatMsg() []chatMsg {
	rand.Seed(time.Now().UnixNano())

	users := []int{100, 200, 300, 400, 500}
	messages := []string{
		"你好啊,最近怎么样?",
		"我最近很好,谢谢关心!",
		"最近工作很忙,有点累了。",
		"注意身体,别太拼了。",
		"好的,我会注意休息的。",
		"周末有时间一起出去玩吗?",
		"可以啊,我们一起去爬山怎么样?",
		"听起来不错,那就这么定了!",
		"对了,你最近在看什么书?",
		"我在看《时间简史》,很有意思。",
	}

	var chatMsgs []chatMsg
	msgId := 1

	startTime := time.Now().Add(-time.Hour * 24 * 7) // 设置起始时间为一周前

	for i := 0; i < 200; i++ {
		uid1 := users[rand.Intn(len(users))]
		uid2 := users[rand.Intn(len(users))]
		for uid2 == uid1 {
			uid2 = users[rand.Intn(len(users))]
		}

		sessionId := fmt.Sprintf("%d_2_%d", uid1, uid2)
		payload := messages[rand.Intn(len(messages))]

		createAt := startTime.Add(time.Duration(rand.Intn(60*60*24*7)) * time.Second) // 随机生成发送时间

		chatMsgs = append(chatMsgs, chatMsg{
			Uid:       uid1,
			SessionId: sessionId,
			MsgId:     msgId,
			Payload:   payload,
			CreateAt:  createAt,
		})

		msgId++
	}

	return chatMsgs
}

func TestRunSlideWindowConsumerGroup_SendMessage(t *testing.T) {
	producer := CreateProducer()

	defer func() {
		if err := producer.Close(); err != nil {
			log.Panicf("Error closing producer: %v", err)
		}
	}()

	topic := "test_slideWindow_partition_chat"
	msgs := generationChatMsg()
	for i := range msgs {
		value, _ := json.Marshal(msgs[i])
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
			Key:   sarama.StringEncoder(msgs[i].SessionId),
		}
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			fmt.Printf("Failed to send message: %v", err)
		} else {
			fmt.Printf("消息发送成功： partition = %d, offset = %d, msg = %s \n", partition, offset, string(value))
		}
	}

}
