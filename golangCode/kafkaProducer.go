package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Proschema struct {
	CommentId string `json:"CommentId"` //uuid as string
	CreatorId string `json:"CreatorId"` //uuid as string
	Comment   string `json:"Comment"`
}

func mainPro(jbyte []byte) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":     "localhost:9092",
		"broker.address.family": "v4",
		"enable.idempotence":    true})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "comments"

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(jbyte),
	}, nil)

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
