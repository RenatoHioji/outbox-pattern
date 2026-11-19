package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type OrderEvent struct {
	ID          string    `json:"id"`
	CustomerID  string    `json:"customer_id"`
	TotalAmount float64   `json:"total_amount"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type EventHandlerFunc func(ctx context.Context, eventID string, eventType string, order OrderEvent) error

type OrderConsumer struct {
	reader    MessageReader
	handler   EventHandlerFunc
	processed sync.Map
}

func NewOrderConsumer(reader MessageReader, handler EventHandlerFunc) *OrderConsumer {
	if handler == nil {
		handler = defaultLogHandler
	}
	return &OrderConsumer{
		reader:  reader,
		handler: handler,
	}
}

func defaultLogHandler(_ context.Context, eventID string, eventType string, order OrderEvent) error {
	log.Printf(
		"[OrderConsumer] Processed event_id=%s event_type=%s order_id=%s customer_id=%s total_amount=%.2f status=%s",
		eventID, eventType, order.ID, order.CustomerID, order.TotalAmount, order.Status,
	)
	return nil
}

func (c *OrderConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("fetch kafka message: %w", err)
		}

		if err := c.ProcessMessage(ctx, msg); err != nil {
			log.Printf("[OrderConsumer] Error processing message key=%s offset=%d: %v", string(msg.Key), msg.Offset, err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("commit kafka message: %w", err)
		}
	}
}

func (c *OrderConsumer) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	eventID := headerValue(msg.Headers, "id")
	eventType := headerValue(msg.Headers, "eventType")

	if eventID != "" {
		if _, alreadyProcessed := c.processed.LoadOrStore(eventID, struct{}{}); alreadyProcessed {
			log.Printf("[OrderConsumer] Skipping duplicate outbox event_id=%s", eventID)
			return nil
		}
	}

	var order OrderEvent
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		if eventID != "" {
			c.processed.Delete(eventID)
		}
		return fmt.Errorf("unmarshal order payload: %w", err)
	}

	if err := c.handler(ctx, eventID, eventType, order); err != nil {
		if eventID != "" {
			c.processed.Delete(eventID)
		}
		return fmt.Errorf("handle order event: %w", err)
	}

	return nil
}

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
