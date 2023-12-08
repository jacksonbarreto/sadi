package consumer

import "github.com/IBM/sarama"

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
}

func NewConsumer(config *sarama.Config, brokerList []string, groupID string) (*Consumer, error) {
	consumerGroup, err := sarama.NewConsumerGroup(brokerList, groupID, config)
	if err != nil {
		return nil, err
	}
	return &Consumer{consumerGroup: consumerGroup}, nil
}

func NewConsumer(brokers []string, group string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{consumerGroup: consumer}, nil
}

func NewConsumerDefault() (*Consumer, error) {
	// Aqui, você obtém as configurações necessárias do seu serviço de configuração
	config, brokerList, groupID := GetKafkaConfigFromService()

	return NewConsumer(config, brokerList, groupID)
}

func (c *Consumer) Consume(topics []string) error {
	handler := &ConsumerGroupHandler{}
	ctx := context.Background()

	for {
		err := c.consumerGroup.Consume(ctx, topics, handler)
		if err != nil {
			return err
		}
	}
}
