package repository

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOrderRepository_CreateOrder(t *testing.T) {
	t.Run("commits both order and outbox message in a single transaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create sqlmock: %v", err)
		}
		defer db.Close()

		repo := NewOrderRepository(db)

		expectedID := "00000000-0000-0000-0000-000000000001"
		expectedTime := time.Date(2026, 11, 16, 22, 0, 0, 0, time.UTC)

		inputOrder := &Order{
			CustomerID:  "cust-123",
			TotalAmount: 99.50,
			Status:      "PENDING",
		}

		expectedPayload, _ := json.Marshal(&Order{
			ID:          expectedID,
			CustomerID:  "cust-123",
			TotalAmount: 99.50,
			Status:      "PENDING",
			CreatedAt:   expectedTime,
		})

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO orders (id, customer_id, total_amount, status) VALUES (gen_random_uuid(), $1, $2, $3) RETURNING id, created_at`)).
			WithArgs("cust-123", 99.50, "PENDING").
			WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(expectedID, expectedTime))

		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox_messages (id, aggregate_type, aggregate_id, event_type, payload) VALUES (gen_random_uuid(), $1, $2, $3, $4)`)).
			WithArgs("Order", expectedID, "OrderCreated", expectedPayload).
			WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectCommit()

		created, err := repo.CreateOrder(context.Background(), inputOrder)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if created.ID != expectedID {
			t.Fatalf("expected ID %q, got %q", expectedID, created.ID)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unfulfilled sql expectations: %v", err)
		}
	})

	t.Run("rolls back transaction if outbox insert fails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create sqlmock: %v", err)
		}
		defer db.Close()

		repo := NewOrderRepository(db)

		expectedID := "00000000-0000-0000-0000-000000000001"
		expectedTime := time.Date(2026, 11, 16, 22, 0, 0, 0, time.UTC)

		inputOrder := &Order{
			CustomerID:  "cust-123",
			TotalAmount: 99.50,
			Status:      "PENDING",
		}

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO orders (id, customer_id, total_amount, status) VALUES (gen_random_uuid(), $1, $2, $3) RETURNING id, created_at`)).
			WithArgs("cust-123", 99.50, "PENDING").
			WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(expectedID, expectedTime))

		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox_messages`)).
			WillReturnError(errors.New("outbox table locked"))

		mock.ExpectRollback()

		created, err := repo.CreateOrder(context.Background(), inputOrder)
		if err == nil {
			t.Fatal("expected error when outbox insert fails, got nil")
		}
		if created != nil {
			t.Fatalf("expected nil order on failure, got %+v", created)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unfulfilled sql expectations: %v", err)
		}
	})
}
