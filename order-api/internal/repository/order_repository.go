package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type Order struct {
	ID          string    `json:"id"`
	CustomerID  string    `json:"customer_id"`
	TotalAmount float64   `json:"total_amount"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

type OrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

func (r *OrderRepository) CreateOrder(ctx context.Context, order *Order) (*Order, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	insertOrderQuery := `
		INSERT INTO orders (id, customer_id, total_amount, status)
		VALUES (gen_random_uuid(), $1, $2, $3)
		RETURNING id, created_at
	`
	if err := tx.QueryRowContext(ctx, insertOrderQuery, order.CustomerID, order.TotalAmount, order.Status).
		Scan(&order.ID, &order.CreatedAt); err != nil {
		return nil, fmt.Errorf("insert order: %w", err)
	}

	payload, err := json.Marshal(order)
	if err != nil {
		return nil, fmt.Errorf("marshal order payload: %w", err)
	}

	insertOutboxQuery := `
		INSERT INTO outbox_messages (id, aggregate_type, aggregate_id, event_type, payload)
		VALUES (gen_random_uuid(), $1, $2, $3, $4)
	`
	if _, err := tx.ExecContext(ctx, insertOutboxQuery, "Order", order.ID, "OrderCreated", payload); err != nil {
		return nil, fmt.Errorf("insert outbox message: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return order, nil
}
