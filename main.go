package main

import (
	"log"
	"net/http"

	"github.com/RenatoHioji/outbox-pattern/internal/controller"
	"github.com/RenatoHioji/outbox-pattern/internal/database"
)

func main() {
	db, err := database.NewPostgresConnection(database.DefaultConfig())
	if err != nil {
		log.Fatalf("database connection failed: %v", err)
	}
	defer db.Close()
	log.Println("Connected to PostgreSQL successfully")

	mux := http.NewServeMux()

	orderController := controller.NewOrderController()
	mux.HandleFunc("POST /orders", orderController.CreateOrder)

	addr := ":8080"
	log.Printf("Starting HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
