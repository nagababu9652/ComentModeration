package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConSchema struct {
	CommentId string `json:"CommentId"` //uuid as string
	CreatorId string `json:"CreatorId"` //uuid as string
	Comment   string `json:"Comment"`
	Polarity  string `json:"Polarity"`
}

func KafkaNewConsumer() {

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     "localhost:9092",
		"group.id":              "myGroup1",
		"broker.address.family": "v4",
		"auto.offset.reset":     "latest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	c.SubscribeTopics([]string{"commentsResult"}, nil)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("Consumed message on %s:%s\n",
					e.TopicPartition, string(e.Value))
				m := ConSchema{}
				json.Unmarshal(e.Value, &m)
				//get partigular attributes from the json now using
				// m.Comment    m.CreatorId    m.CommentId    m.Polarity

				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, e)
			default:
				fmt.Printf("Ignore: %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
