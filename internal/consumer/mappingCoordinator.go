package consumer

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"log"
)

type KafkaMessage struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}
type ProcessorMap map[string]Processor

type Processor interface {
	Process(payload string)
}

type MappingCoordinator struct {
	processors ProcessorMap
}

func NewMappingCoordinator(processorMap ProcessorMap) *MappingCoordinator {
	return &MappingCoordinator{
		processors: processorMap,
	}
}

func (m *MappingCoordinator) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (m *MappingCoordinator) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (m *MappingCoordinator) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var kafkaMessage KafkaMessage
		if err := json.Unmarshal(message.Value, &kafkaMessage); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		if processor, exists := m.processors[kafkaMessage.Type]; exists {
			go func(originalMsg *sarama.ConsumerMessage, kafkaMsg KafkaMessage) {
				processor.Process(kafkaMsg.Payload)
				session.MarkMessage(originalMsg, "")
			}(message, kafkaMessage)
		} else {
			// TODO: Add a dead letter queue
			// TODO: Send a message to topic "dead-letter-queue" with the message
			log.Printf("No processor found for message type %s", kafkaMessage.Type)
		}
	}
	return nil
}
