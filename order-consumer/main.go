package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/RenatoHioji/outbox-pattern/order-consumer/internal/consumer"
	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getEnv("KAFKA_TOPIC", "outbox.event.Order")
	groupID := getEnv("KAFKA_GROUP_ID", "order-consumer-group")

	ensureTopicExists(brokers[0], topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               brokers,
		Topic:                 topic,
		GroupID:               groupID,
		StartOffset:           kafka.FirstOffset,
		WatchPartitionChanges: true,
		ErrorLogger:           kafka.LoggerFunc(log.Printf),
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	orderConsumer := consumer.NewOrderConsumer(reader, nil)

	log.Printf("Starting Order Kafka Consumer (brokers=%v, topic=%s, group=%s)", brokers, topic, groupID)
	if err := orderConsumer.Run(ctx); err != nil {
		log.Fatalf("consumer stopped with error: %v", err)
	}
	log.Println("Order Kafka Consumer shut down cleanly")
}

func ensureTopicExists(broker, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Printf("warning: could not dial kafka broker %s to check topic: %v", broker, err)
		return
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Printf("warning: could not ensure topic %s exists: %v", topic, err)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
