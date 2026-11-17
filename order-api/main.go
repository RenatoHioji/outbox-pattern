package main

import (
	"log"
	"net/http"

	"github.com/RenatoHioji/outbox-pattern/order-api/internal/controller"
	"github.com/RenatoHioji/outbox-pattern/order-api/internal/database"
	"github.com/RenatoHioji/outbox-pattern/order-api/internal/repository"
	"github.com/RenatoHioji/outbox-pattern/order-api/internal/service"
)

func main() {
	db, err := database.NewPostgresConnection(database.DefaultConfig())
	if err != nil {
		log.Fatalf("database connection failed: %v", err)
	}
	defer db.Close()
	log.Println("Connected to PostgreSQL successfully")

	orderRepo := repository.NewOrderRepository(db)
	orderService := service.NewOrderService(orderRepo)
	orderController := controller.NewOrderController(orderService)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", orderController.CreateOrder)

	addr := ":8080"
	log.Printf("Starting HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
