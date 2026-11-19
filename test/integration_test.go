package test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Order struct {
	ID          string    `json:"id"`
	CustomerID  string    `json:"customer_id"`
	TotalAmount float64   `json:"total_amount"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

func openPostgres(t *testing.T) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "outbox_user"),
		getEnv("POSTGRES_PASSWORD", "outbox_password"),
		getEnv("POSTGRES_DB", "outbox_db"),
		getEnv("POSTGRES_SSLMODE", "disable"),
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("failed to ping postgres: %v", err)
	}

	initSQL, err := os.ReadFile(filepath.Join("..", "init.sql"))
	if err != nil {
		t.Fatalf("failed to read ../init.sql: %v", err)
	}
	if _, err := db.ExecContext(ctx, string(initSQL)); err != nil {
		t.Fatalf("failed to apply init.sql schema: %v", err)
	}

	return db
}

func ensureOrderAPI(t *testing.T) string {
	t.Helper()

	apiURL := getEnv("ORDER_API_URL", "http://localhost:8080")
	if isPortOpen("localhost:8080") {
		return apiURL
	}

	binPath := filepath.Join(t.TempDir(), "order-api.exe")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildCmd.Dir = filepath.Join("..", "order-api")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build order-api: %v\n%s", err, string(out))
	}

	cmd := exec.Command(binPath)
	cmd.Dir = filepath.Join("..", "order-api")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start order-api: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if isPortOpen("localhost:8080") {
			return "http://localhost:8080"
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("timed out waiting for order-api to listen on localhost:8080")
	return ""
}

func isPortOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func ensureTopicExists(t *testing.T, broker, topic string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("failed to dial kafka broker %s: %v", broker, err)
	}
	defer conn.Close()

	_ = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
}

func ensureDebeziumConnector(t *testing.T) {
	t.Helper()

	debeziumURL := getEnv("DEBEZIUM_URL", "http://localhost:8083")
	statusResp, err := http.Get(debeziumURL + "/connectors/outbox-connector/status")
	if err == nil {
		_ = statusResp.Body.Close()
		if statusResp.StatusCode == http.StatusOK {
			return
		}
	}

	cfgBytes, err := os.ReadFile(filepath.Join("..", "debezium", "outbox-connector.json"))
	if err != nil {
		t.Fatalf("failed to read ../debezium/outbox-connector.json: %v", err)
	}

	if dbHost := os.Getenv("DEBEZIUM_DB_HOST"); dbHost != "" {
		cfgBytes = bytes.ReplaceAll(
			cfgBytes,
			[]byte(`"database.hostname": "postgres"`),
			[]byte(fmt.Sprintf(`"database.hostname": %q`, dbHost)),
		)
	}

	resp, err := http.Post(debeziumURL+"/connectors", "application/json", bytes.NewReader(cfgBytes))
	if err != nil {
		t.Fatalf("failed to register Debezium connector: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		t.Fatalf("unexpected status registering Debezium connector: %d", resp.StatusCode)
	}
}

func TestOutboxDatabaseIntegration(t *testing.T) {
	db := openPostgres(t)
	apiURL := ensureOrderAPI(t)

	customerID := fmt.Sprintf("cust-it-%d", time.Now().UnixNano())
	reqBody := fmt.Sprintf(`{"customer_id":%q,"total_amount":149.95}`, customerID)

	resp, err := http.Post(apiURL+"/orders", "application/json", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("POST /orders failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d", resp.StatusCode)
	}

	var created Order
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Verify row in orders table
	var dbCustomerID, dbStatus string
	var dbTotalAmount float64
	err = db.QueryRowContext(
		ctx,
		`SELECT customer_id, total_amount, status FROM orders WHERE id = $1`,
		created.ID,
	).Scan(&dbCustomerID, &dbTotalAmount, &dbStatus)
	if err != nil {
		t.Fatalf("failed to query orders table for id=%s: %v", created.ID, err)
	}

	if dbCustomerID != customerID || dbTotalAmount != 149.95 || dbStatus != "PENDING" {
		t.Fatalf("unexpected order row in DB: customer_id=%q total_amount=%v status=%q", dbCustomerID, dbTotalAmount, dbStatus)
	}

	// 2. Verify row in outbox_messages table
	var outboxID, aggregateType, eventType string
	var rawPayload []byte
	err = db.QueryRowContext(
		ctx,
		`SELECT id, aggregate_type, event_type, payload FROM outbox_messages WHERE aggregate_id = $1`,
		created.ID,
	).Scan(&outboxID, &aggregateType, &eventType, &rawPayload)
	if err != nil {
		t.Fatalf("failed to query outbox_messages table for aggregate_id=%s: %v", created.ID, err)
	}

	if outboxID == "" || aggregateType != "Order" || eventType != "OrderCreated" {
		t.Fatalf("unexpected outbox row: id=%q aggregate_type=%q event_type=%q", outboxID, aggregateType, eventType)
	}

	var outboxOrder Order
	if err := json.Unmarshal(rawPayload, &outboxOrder); err != nil {
		t.Fatalf("failed to unmarshal outbox payload JSONB: %v", err)
	}
	if outboxOrder.ID != created.ID || outboxOrder.CustomerID != customerID || outboxOrder.TotalAmount != 149.95 {
		t.Fatalf("outbox payload mismatch: got %+v, want %+v", outboxOrder, created)
	}
}

func TestDebeziumCDCEndToEndIntegration(t *testing.T) {
	_ = openPostgres(t)
	ensureDebeziumConnector(t)
	apiURL := ensureOrderAPI(t)

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getEnv("KAFKA_TOPIC", "outbox.event.Order")
	groupID := fmt.Sprintf("cdc-e2e-group-%d", time.Now().UnixNano())

	ensureTopicExists(t, brokers[0], topic)

	customerID := fmt.Sprintf("cust-cdc-%d", time.Now().UnixNano())
	reqBody := fmt.Sprintf(`{"customer_id":%q,"total_amount":275.50}`, customerID)

	resp, err := http.Post(apiURL+"/orders", "application/json", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("POST /orders failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created from order-api, got %d", resp.StatusCode)
	}

	var created Order
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode order-api response: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               brokers,
		Topic:                 topic,
		GroupID:               groupID,
		StartOffset:           kafka.FirstOffset,
		WatchPartitionChanges: true,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("failed waiting for Debezium CDC event for order_id=%s: %v", created.ID, err)
		}

		var event Order
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			t.Fatalf("failed to unmarshal Kafka message value: %v", err)
		}

		if event.ID != created.ID {
			continue
		}

		eventID := headerValue(msg.Headers, "id")
		eventType := headerValue(msg.Headers, "eventType")

		if string(msg.Key) != created.ID {
			t.Fatalf("expected Kafka message key %q, got %q", created.ID, string(msg.Key))
		}
		if eventID == "" {
			t.Fatal("expected Debezium outbox 'id' header to be non-empty")
		}
		if eventType != "OrderCreated" {
			t.Fatalf("expected Debezium 'eventType' header 'OrderCreated', got %q", eventType)
		}
		if event.CustomerID != customerID || event.TotalAmount != 275.50 || event.Status != "PENDING" {
			t.Fatalf("CDC event payload mismatch: got %+v", event)
		}
		break
	}
}

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
