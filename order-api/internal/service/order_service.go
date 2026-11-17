package service

import (
	"context"

	"github.com/RenatoHioji/outbox-pattern/order-api/internal/repository"
)

type OrderRepository interface {
	CreateOrder(ctx context.Context, order *repository.Order) (*repository.Order, error)
}

type OrderService struct {
	repo OrderRepository
}

func NewOrderService(repo OrderRepository) *OrderService {
	return &OrderService{repo: repo}
}

func (s *OrderService) CreateOrder(ctx context.Context, customerID string, totalAmount float64) (*repository.Order, error) {
	order := &repository.Order{
		CustomerID:  customerID,
		TotalAmount: totalAmount,
		Status:      "PENDING",
	}

	return s.repo.CreateOrder(ctx, order)
}
