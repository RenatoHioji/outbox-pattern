package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/RenatoHioji/outbox-pattern/order-api/internal/repository"
)

type mockOrderStore struct{}

func (m *mockOrderStore) CreateOrder(_ context.Context, customerID string, totalAmount float64) (*repository.Order, error) {
	return &repository.Order{
		ID:          "00000000-0000-0000-0000-000000000001",
		CustomerID:  customerID,
		TotalAmount: totalAmount,
		Status:      "PENDING",
		CreatedAt:   time.Date(2026, 11, 16, 21, 0, 0, 0, time.UTC),
	}, nil
}

func TestCreateOrder(t *testing.T) {
	mux := http.NewServeMux()
	orderController := NewOrderController(&mockOrderStore{})
	mux.HandleFunc("POST /orders", orderController.CreateOrder)

	validResponseBytes, _ := json.Marshal(&repository.Order{
		ID:          "00000000-0000-0000-0000-000000000001",
		CustomerID:  "cust-123",
		TotalAmount: 99.50,
		Status:      "PENDING",
		CreatedAt:   time.Date(2026, 11, 16, 21, 0, 0, 0, time.UTC),
	})

	tests := []struct {
		name           string
		method         string
		path           string
		body           string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "valid order payload returns 201 created with order JSON",
			method:         http.MethodPost,
			path:           "/orders",
			body:           `{"customer_id":"cust-123","total_amount":99.50}`,
			expectedStatus: http.StatusCreated,
			expectedBody:   string(validResponseBytes) + "\n",
		},
		{
			name:           "missing customer_id fails struct validation with 400",
			method:         http.MethodPost,
			path:           "/orders",
			body:           `{"total_amount":99.50}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "non-positive total_amount fails struct validation with 400",
			method:         http.MethodPost,
			path:           "/orders",
			body:           `{"customer_id":"cust-123","total_amount":0}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "invalid JSON returns 400 bad request",
			method:         http.MethodPost,
			path:           "/orders",
			body:           `{"customer_id":`,
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "invalid JSON payload\n",
		},
		{
			name:           "empty body returns 400 bad request",
			method:         http.MethodPost,
			path:           "/orders",
			body:           ``,
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "invalid JSON payload\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d", tc.expectedStatus, rec.Code)
			}

			if tc.expectedBody != "" && rec.Body.String() != tc.expectedBody {
				t.Fatalf("expected body %q, got %q", tc.expectedBody, rec.Body.String())
			}
		})
	}
}
