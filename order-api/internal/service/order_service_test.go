package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/RenatoHioji/outbox-pattern/order-api/internal/repository"
)

type mockOrderRepository struct {
	receivedOrder *repository.Order
	err           error
}

func (m *mockOrderRepository) CreateOrder(_ context.Context, order *repository.Order) (*repository.Order, error) {
	m.receivedOrder = order
	if m.err != nil {
		return nil, m.err
	}

	created := *order
	created.ID = "00000000-0000-0000-0000-000000000001"
	created.CreatedAt = time.Date(2026, 11, 16, 22, 0, 0, 0, time.UTC)
	return &created, nil
}

func TestOrderService_CreateOrder(t *testing.T) {
	t.Run("sets default PENDING status and delegates to repository", func(t *testing.T) {
		mockRepo := &mockOrderRepository{}
		svc := NewOrderService(mockRepo)

		order, err := svc.CreateOrder(context.Background(), "cust-123", 150.75)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if mockRepo.receivedOrder == nil {
			t.Fatal("expected repository CreateOrder to be called")
		}
		if mockRepo.receivedOrder.Status != "PENDING" {
			t.Fatalf("expected status PENDING, got %q", mockRepo.receivedOrder.Status)
		}
		if order.ID == "" {
			t.Fatal("expected created order to have an ID")
		}
		if order.CustomerID != "cust-123" || order.TotalAmount != 150.75 {
			t.Fatalf("unexpected order fields: %+v", order)
		}
	})

	t.Run("propagates repository error", func(t *testing.T) {
		expectedErr := errors.New("db connection lost")
		mockRepo := &mockOrderRepository{err: expectedErr}
		svc := NewOrderService(mockRepo)

		order, err := svc.CreateOrder(context.Background(), "cust-123", 150.75)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
		if order != nil {
			t.Fatalf("expected nil order on error, got %+v", order)
		}
	})
}
