package test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/redis/go-redis/v9"
	"github.com/wesleyskap/orkai-runiq/v3/queue"
)

func TestStoragePluginSystem(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	store, err := queue.OpenStorage("sqlite", db)
	if err != nil {
		t.Fatalf("failed to open sqlite store via plugin system: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestLeaderElectionSqlite(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	store, _ := queue.NewSqliteStorage(db)
	runLeaderElectionTest(t, store)
}

func TestNamespacesSqlite(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	store1, _ := queue.NewSqliteStorage(db)
	store2, _ := queue.NewSqliteStorage(db)
	runNamespaceIsolationTest(t, store1, store2)
}

func TestArchivalSqlite(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	store, _ := queue.NewSqliteStorage(db)
	runArchivalTest(t, store)
}

func TestLeaderElectionPostgres(t *testing.T) {
	db, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		t.Skip("skipping postgres, connection failed")
		return
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skip("skipping postgres, ping failed")
		return
	}
	_, _ = db.Exec("DELETE FROM runiq_leader_leases")
	store, _ := queue.NewPostgresStorage(db)
	runLeaderElectionTest(t, store)
}

func TestNamespacesPostgres(t *testing.T) {
	db, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		t.Skip("skipping postgres, connection failed")
		return
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skip("skipping postgres, ping failed")
		return
	}
	store1, _ := queue.NewPostgresStorage(db)
	store2, _ := queue.NewPostgresStorage(db)
	runNamespaceIsolationTest(t, store1, store2)
}

func TestArchivalPostgres(t *testing.T) {
	db, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		t.Skip("skipping postgres, connection failed")
		return
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skip("skipping postgres, ping failed")
		return
	}
	store, _ := queue.NewPostgresStorage(db)
	runArchivalTest(t, store)
}

func TestLeaderElectionRedis(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skip("skipping redis, ping failed")
		return
	}
	client.FlushAll(context.Background())
	store, _ := queue.NewRedisStorage(client)
	runLeaderElectionTest(t, store)
}

func TestNamespacesRedis(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skip("skipping redis, ping failed")
		return
	}
	store1, _ := queue.NewRedisStorage(client)
	store2, _ := queue.NewRedisStorage(client)
	runNamespaceIsolationTest(t, store1, store2)
}

func TestArchivalRedis(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skip("skipping redis, ping failed")
		return
	}
	store, _ := queue.NewRedisStorage(client)
	runArchivalTest(t, store)
}

func runLeaderElectionTest(t *testing.T, store queue.WorkerPoolStorage) {
	ctx := context.Background()
	ok1, err1 := store.AcquireLeader(ctx, "client-a", 100*time.Millisecond)
	if err1 != nil || !ok1 {
		t.Fatalf("client-a failed to acquire: ok=%v, err=%v", ok1, err1)
	}
	ok2, err2 := store.AcquireLeader(ctx, "client-b", 100*time.Millisecond)
	if err2 != nil || ok2 {
		t.Fatalf("client-b acquired unexpectedly: ok=%v, err=%v", ok2, err2)
	}
	time.Sleep(120 * time.Millisecond)
	ok3, err3 := store.AcquireLeader(ctx, "client-b", 100*time.Millisecond)
	if err3 != nil || !ok3 {
		t.Fatalf("client-b failed to acquire after TTL: ok=%v, err=%v", ok3, err3)
	}
	_ = store.ReleaseLeader(ctx, "client-b")
}

func runNamespaceIsolationTest(t *testing.T, store1, store2 queue.WorkerPoolStorage) {
	ctx := context.Background()
	store1.(queue.Namespacer).SetNamespace("tenant1")
	store2.(queue.Namespacer).SetNamespace("tenant2")
	env := &queue.JobEnvelope{
		JobID: "job-ns-1", Queue: "q1", Name: "JobNs", Args: []byte("{}"),
	}
	if err := store1.Enqueue(ctx, env); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	assertNsIsolation(t, ctx, store1, store2, env.JobID)
}

func assertNsIsolation(t *testing.T, ctx context.Context, store1, store2 queue.WorkerPoolStorage, jobID string) {
	deq1, err1 := store1.Dequeue(ctx, "q1")
	if err1 != nil || deq1 == nil || deq1.JobID != jobID {
		t.Fatalf("store1 failed to dequeue: %v, %v", deq1, err1)
	}
	deq2, err2 := store2.Dequeue(ctx, "q1")
	if err2 != nil || deq2 != nil {
		t.Fatalf("store2 dequeued cross-tenant job: %v, %v", deq2, err2)
	}
}

func runArchivalTest(t *testing.T, store queue.WorkerPoolStorage) {
	ctx := context.Background()
	env := &queue.JobEnvelope{
		JobID: "job-arc-1", Queue: "q2", Name: "JobArc", Args: []byte("{}"),
	}
	_ = store.Enqueue(ctx, env)
	deq, _ := store.Dequeue(ctx, "q2")
	_ = store.Ack(ctx, deq.JobID)
	count, err := store.ArchiveJobs(ctx, 0)
	if err != nil || count != 1 {
		t.Fatalf("failed to archive: count=%d, err=%v", count, err)
	}
	assertJobArchived(t, ctx, store, env.JobID)
}

func assertJobArchived(t *testing.T, ctx context.Context, store queue.WorkerPoolStorage, jobID string) {
	deq, err := store.Dequeue(ctx, "q2")
	if err != nil || deq != nil {
		t.Fatalf("job still active after archiving: %v, %v", deq, err)
	}
}
