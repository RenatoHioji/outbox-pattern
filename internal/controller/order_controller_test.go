package controller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateOrder(t *testing.T) {
	mux := http.NewServeMux()
	orderController := NewOrderController()
	mux.HandleFunc("POST /orders", orderController.CreateOrder)

	tests := []struct {
		name           string
		method         string
		path           string
		body           string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "valid order payload returns 200 received",
			method:         http.MethodPost,
			path:           "/orders",
			body:           `{"customer_id":"cust-123","total_amount":99.50}`,
			expectedStatus: http.StatusOK,
			expectedBody:   "received",
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

			if rec.Body.String() != tc.expectedBody {
				t.Fatalf("expected body %q, got %q", tc.expectedBody, rec.Body.String())
			}
		})
	}
}
