package controller

import (
	"encoding/json"
	"net/http"
)

type CreateOrderRequest struct {
	CustomerID  string  `json:"customer_id"`
	TotalAmount float64 `json:"total_amount"`
}

type OrderController struct{}

func NewOrderController() *OrderController {
	return &OrderController{}
}

func (c *OrderController) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("received"))
}
