package controller

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/RenatoHioji/outbox-pattern/internal/repository"
	"github.com/go-playground/validator/v10"
)

type OrderService interface {
	CreateOrder(ctx context.Context, customerID string, totalAmount float64) (*repository.Order, error)
}

type CreateOrderRequest struct {
	CustomerID  string  `json:"customer_id" validate:"required"`
	TotalAmount float64 `json:"total_amount" validate:"required,gt=0"`
}

type OrderController struct {
	service  OrderService
	validate *validator.Validate
}

func NewOrderController(svc OrderService) *OrderController {
	return &OrderController{
		service:  svc,
		validate: validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (c *OrderController) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	if err := c.validate.Struct(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	order, err := c.service.CreateOrder(r.Context(), req.CustomerID, req.TotalAmount)
	if err != nil {
		http.Error(w, "failed to create order", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(order)
}
