package main

import (
	"log"
	"net/http"

	"github.com/RenatoHioji/outbox-pattern/internal/controller"
)

func main() {
	mux := http.NewServeMux()

	orderController := controller.NewOrderController()
	mux.HandleFunc("POST /orders", orderController.CreateOrder)

	addr := ":8080"
	log.Printf("Starting HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
