package consumer

import (
	"github.com/IBM/sarama"
	config "github.com/jacksonbarreto/sadi/config"
)

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
}

func NewConsumer(brokers []string, group string, topics []string) (*Consumer, error) {
	configConsumerGroup := sarama.NewConfig()
	configConsumerGroup.Version = sarama.V2_0_0_0
	configConsumerGroup.Consumer.Return.Errors = true
	configConsumerGroup.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(brokers, group, configConsumerGroup)
	if err != nil {
		return nil, err
	}

	return &Consumer{consumerGroup: consumer, topics: topics}, nil
}

func NewConsumerDefault() (*Consumer, error) {
	kafkaConfig := config.Kafka()
	brokerList := kafkaConfig.Brokers
	groupID := kafkaConfig.GroupID
	topics := kafkaConfig.Topics

	return NewConsumer(brokerList, groupID, topics)
}

func (c *Consumer) Consume() error {
	handler := &ConsumerGroupHandler{}
	ctx := context.Background()

	for {
		err := c.consumerGroup.Consume(ctx, c.topics, handler)
		if err != nil {
			return err
		}
	}
}
