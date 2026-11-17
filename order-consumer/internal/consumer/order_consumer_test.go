package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestOrderConsumer_ProcessMessage(t *testing.T) {
	t.Run("parses Debezium Outbox EventRouter message and invokes handler", func(t *testing.T) {
		var capturedID, capturedType string
		var capturedOrder OrderEvent

		c := NewOrderConsumer(nil, func(_ context.Context, eventID, eventType string, order OrderEvent) error {
			capturedID = eventID
			capturedType = eventType
			capturedOrder = order
			return nil
		})

		msg := kafka.Message{
			Key: []byte("00000000-0000-0000-0000-000000000001"),
			Headers: []kafka.Header{
				{Key: "id", Value: []byte("outbox-msg-001")},
				{Key: "eventType", Value: []byte("OrderCreated")},
			},
			Value: []byte(`{"id":"00000000-0000-0000-0000-000000000001","customer_id":"cust-123","total_amount":99.5,"status":"PENDING","created_at":"2026-11-17T18:00:00Z"}`),
		}

		if err := c.ProcessMessage(context.Background(), msg); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if capturedID != "outbox-msg-001" {
			t.Fatalf("expected eventID %q, got %q", "outbox-msg-001", capturedID)
		}
		if capturedType != "OrderCreated" {
			t.Fatalf("expected eventType %q, got %q", "OrderCreated", capturedType)
		}
		if capturedOrder.ID != "00000000-0000-0000-0000-000000000001" || capturedOrder.CustomerID != "cust-123" || capturedOrder.TotalAmount != 99.5 {
			t.Fatalf("unexpected order payload: %+v", capturedOrder)
		}
		if !capturedOrder.CreatedAt.Equal(time.Date(2026, 11, 17, 18, 0, 0, 0, time.UTC)) {
			t.Fatalf("unexpected created_at: %v", capturedOrder.CreatedAt)
		}
	})

	t.Run("deduplicates repeated outbox messages with the same id header", func(t *testing.T) {
		callCount := 0
		c := NewOrderConsumer(nil, func(_ context.Context, _, _ string, _ OrderEvent) error {
			callCount++
			return nil
		})

		msg := kafka.Message{
			Key: []byte("00000000-0000-0000-0000-000000000001"),
			Headers: []kafka.Header{
				{Key: "id", Value: []byte("outbox-msg-dup")},
				{Key: "eventType", Value: []byte("OrderCreated")},
			},
			Value: []byte(`{"id":"00000000-0000-0000-0000-000000000001","customer_id":"cust-123","total_amount":50,"status":"PENDING","created_at":"2026-11-17T18:00:00Z"}`),
		}

		if err := c.ProcessMessage(context.Background(), msg); err != nil {
			t.Fatalf("first process failed: %v", err)
		}
		if err := c.ProcessMessage(context.Background(), msg); err != nil {
			t.Fatalf("duplicate process failed: %v", err)
		}

		if callCount != 1 {
			t.Fatalf("expected handler to be called once for duplicate event id, got %d", callCount)
		}
	})

	t.Run("returns error on invalid JSON payload", func(t *testing.T) {
		c := NewOrderConsumer(nil, nil)

		msg := kafka.Message{
			Headers: []kafka.Header{{Key: "id", Value: []byte("outbox-msg-bad")}},
			Value:   []byte(`{"id":`),
		}

		if err := c.ProcessMessage(context.Background(), msg); err == nil {
			t.Fatal("expected error on invalid JSON payload, got nil")
		}
	})
}
