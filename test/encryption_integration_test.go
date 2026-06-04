package test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/wesleyskap/orkai-runiq/v3/queue"
)

type mockJob struct {
	performChan chan []byte
	errChan     chan error
}

func (m *mockJob) Perform(ctx context.Context, args []byte) error {
	m.performChan <- args
	return <-m.errChan
}

func TestEncryptionIntegration(t *testing.T) {
	db, store := setupSqlite(t)
	defer db.Close()

	key := []byte("12345678901234567890123456789012") // 32 bytes
	client := queue.NewClient(store, queue.WithClientPayloadEncryption(key))
	ctx := context.Background()

	plaintextPayload := []byte("highly-sensitive-personal-data")
	err := client.Enqueue(ctx, "enc-queue", "SensitiveJob", plaintextPayload)
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	verifyStoredDataIsEncrypted(t, db)

	// Set up worker pool with the decryption key
	pool := queue.NewWorkerPool(store, 1, queue.WithWorkerPayloadEncryption(key))
	mock := &mockJob{
		performChan: make(chan []byte, 1),
		errChan:     make(chan error, 1),
	}
	pool.Register("SensitiveJob", mock)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		_ = pool.Start(workerCtx, "enc-queue")
	}()

	select {
	case received := <-mock.performChan:
		if string(received) != string(plaintextPayload) {
			t.Errorf("expected %s, got %s", plaintextPayload, received)
		}
		mock.errChan <- nil
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for job to perform")
	}
}

func verifyStoredDataIsEncrypted(t *testing.T, db *sql.DB) {
	var args []byte
	err := db.QueryRow("SELECT args FROM runiq_jobs WHERE name = 'SensitiveJob'").Scan(&args)
	if err != nil {
		t.Fatalf("failed to query job arguments from database: %v", err)
	}

	if !queue.IsEncrypted(args) {
		t.Errorf("expected payload in database to be encrypted, got plain text: %s", args)
	}
}

func TestEncryptionIntegrationMissingKey(t *testing.T) {
	db, store := setupSqlite(t)
	defer db.Close()

	key := []byte("12345678901234567890123456789012")
	client := queue.NewClient(store, queue.WithClientPayloadEncryption(key))
	ctx := context.Background()

	err := client.Enqueue(ctx, "enc-queue-nokey", "SensitiveJobNoKey", []byte("secret"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// WorkerPool without encryption/decryption key configuration
	pool := queue.NewWorkerPool(store, 1)
	mock := &mockJob{
		performChan: make(chan []byte, 1),
		errChan:     make(chan error, 1),
	}
	pool.Register("SensitiveJobNoKey", mock)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		_ = pool.Start(workerCtx, "enc-queue-nokey")
	}()

	// Since it fails immediately due to missing key, let's wait a bit and check DB job status
	time.Sleep(200 * time.Millisecond)

	var status, lastError string
	err = db.QueryRow("SELECT status, error_message FROM runiq_jobs WHERE name = 'SensitiveJobNoKey'").Scan(&status, &lastError)
	if err != nil {
		t.Fatalf("failed to query job status: %v", err)
	}

	if status != "pending" {
		t.Errorf("expected job status 'pending', got '%s'", status)
	}

	expectedErr := "payload is encrypted but worker has no key configured"
	if lastError != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, lastError)
	}
}
