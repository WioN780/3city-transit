package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Publisher publishes GPSPosition records to Kafka, one message per record,
// keyed by vehicle_id so a given vehicle's positions land on one partition.
type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Publisher) Publish(ctx context.Context, records []GPSPosition) error {
	if len(records) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(records))
	for _, r := range records {
		value, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal record for vehicle %s: %w", r.VehicleID, err)
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(r.VehicleID),
			Value: value,
		})
	}

	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("write messages: %w", err)
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
