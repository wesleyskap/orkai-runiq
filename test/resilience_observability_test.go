package test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/redis/go-redis/v9"
	"github.com/wesleyskap/orkai-runiq/v2/queue"
)

type fnJob struct {
	fn func(ctx context.Context, args []byte) error
}

func (j *fnJob) Perform(ctx context.Context, args []byte) error {
	err := j.fn(ctx, args)
	return err
}

func TestWorkerPoolMiddleware(t *testing.T) {
	fakeStore := &FakeStorage{}
	pool := queue.NewWorkerPool(fakeStore, 1)
	var runOrder []string
	pool.Use(func(next queue.JobHandler) queue.JobHandler {
		return func(ctx context.Context, env *queue.JobEnvelope) error {
			runOrder = append(runOrder, "mw1")
			return next(context.WithValue(ctx, "k1", "v1"), env)
		}
	}, func(next queue.JobHandler) queue.JobHandler {
		return func(ctx context.Context, env *queue.JobEnvelope) error {
			runOrder = append(runOrder, "mw2")
			v := ctx.Value("k1").(string)
			return next(context.WithValue(ctx, "k2", v+"-v2"), env)
		}
	})
	assertMiddlewareExec(t, pool, fakeStore, &runOrder)
}

func assertMiddlewareExec(t *testing.T, pool *queue.WorkerPool, fakeStore *FakeStorage, runOrder *[]string) {
	var finalVal string
	pool.Register("MJob", &fnJob{fn: func(ctx context.Context, args []byte) error {
		*runOrder = append(*runOrder, "job")
		finalVal = ctx.Value("k2").(string)
		return nil
	}})
	env := &queue.JobEnvelope{JobID: "j-mw", Queue: "default", Name: "MJob", Args: []byte("{}")}
	_ = fakeStore.Enqueue(context.Background(), env)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_ = pool.Start(ctx, "default")
	if len(*runOrder) != 3 || (*runOrder)[0] != "mw1" || (*runOrder)[1] != "mw2" || (*runOrder)[2] != "job" {
		t.Errorf("unexpected run order: %v", *runOrder)
	}
	if finalVal != "v1-v2" {
		t.Errorf("expected finalVal 'v1-v2', got %q", finalVal)
	}
}

func TestEventCompleted(t *testing.T) {
	fakeStore := &FakeStorage{}
	pool := queue.NewWorkerPool(fakeStore, 1)
	var completed bool
	pool.OnEvent(queue.EventJobCompleted, func(ev queue.Event) { completed = true })
	pool.Register("OkJob", &fnJob{fn: func(ctx context.Context, args []byte) error { return nil }})
	env := &queue.JobEnvelope{JobID: "j-ok", Queue: "default", Name: "OkJob", Args: []byte("{}")}
	_ = fakeStore.Enqueue(context.Background(), env)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_ = pool.Start(ctx, "default")
	if !completed {
		t.Error("expected EventJobCompleted")
	}
}

func TestEventFailed(t *testing.T) {
	fakeStore := &FakeStorage{}
	pool := queue.NewWorkerPool(fakeStore, 1)
	var failed bool
	pool.OnEvent(queue.EventJobFailed, func(ev queue.Event) { failed = true })
	pool.Register("FailJob", &fnJob{fn: func(ctx context.Context, args []byte) error { return fmt.Errorf("err") }})
	env := &queue.JobEnvelope{JobID: "j-fail", Queue: "default", Name: "FailJob", Args: []byte("{}"), MaxAttempts: 3, Attempts: 0}
	_ = fakeStore.Enqueue(context.Background(), env)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_ = pool.Start(ctx, "default")
	if !failed {
		t.Error("expected EventJobFailed")
	}
}

func TestEventDead(t *testing.T) {
	fakeStore := &FakeStorage{}
	pool := queue.NewWorkerPool(fakeStore, 1)
	var dead bool
	pool.OnEvent(queue.EventJobDead, func(ev queue.Event) { dead = true })
	pool.Register("FailJob", &fnJob{fn: func(ctx context.Context, args []byte) error { return fmt.Errorf("err") }})
	env := &queue.JobEnvelope{JobID: "j-dead", Queue: "default", Name: "FailJob", Args: []byte("{}"), MaxAttempts: 3, Attempts: 2}
	_ = fakeStore.Enqueue(context.Background(), env)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_ = pool.Start(ctx, "default")
	if !dead {
		t.Error("expected EventJobDead")
	}
}

func TestSqliteDLQPurge(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	s, err := queue.NewSqliteStorage(db)
	if err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	ctx := context.Background()
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	assertSqliteDLQPurgeLogic(t, ctx, s, db)
}

func assertSqliteDLQPurgeLogic(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	env := &queue.JobEnvelope{JobID: "j-dead", Queue: "default", Name: "Job", Args: []byte("{}")}
	_ = s.Enqueue(ctx, env)
	_, _ = db.Exec("UPDATE runiq_jobs SET status = 'dead', updated_at = ? WHERE job_id = ?", time.Now().Add(-2*time.Hour), env.JobID)
	if err := s.PurgeExpiredDLQ(ctx, 1*time.Hour); err != nil {
		t.Fatalf("purge failed: %v", err)
	}
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE job_id = ?", env.JobID).Scan(&count)
	if count != 0 {
		t.Errorf("expected dead job to be purged, found in db")
	}
}

func TestPostgresDLQPurge(t *testing.T) {
	db, err := sql.Open("postgres", postgresConnStr)
	if err != nil || db.Ping() != nil {
		t.Skip("skipping postgres storage tests, service unreachable")
		return
	}
	defer db.Close()
	s, err := queue.NewPostgresStorage(db)
	if err != nil {
		t.Fatalf("failed to init: %v", err)
	}
	ctx := context.Background()
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	assertPostgresDLQPurgeLogic(t, ctx, s, db)
}

func assertPostgresDLQPurgeLogic(t *testing.T, ctx context.Context, s *queue.PostgresStorage, db *sql.DB) {
	_, _ = db.Exec("DELETE FROM runiq_jobs WHERE job_id = 'j-dead-pg'")
	env := &queue.JobEnvelope{JobID: "j-dead-pg", Queue: "default", Name: "Job", Args: []byte("{}")}
	_ = s.Enqueue(ctx, env)
	_, _ = db.Exec("UPDATE runiq_jobs SET status = 'dead', updated_at = $1 WHERE job_id = $2", time.Now().Add(-2*time.Hour), env.JobID)
	if err := s.PurgeExpiredDLQ(ctx, 1*time.Hour); err != nil {
		t.Fatalf("purge failed: %v", err)
	}
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE job_id = $1", env.JobID).Scan(&count)
	if count != 0 {
		t.Errorf("expected dead job to be purged, found in db")
	}
}

func TestRedisDLQPurge(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("skipping redis tests, service unreachable")
		return
	}
	s, err := queue.NewRedisStorage(client)
	if err != nil {
		t.Fatalf("failed to init: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	assertRedisDLQPurgeLogic(t, ctx, s, client)
}

func assertRedisDLQPurgeLogic(t *testing.T, ctx context.Context, s *queue.RedisStorage, client *redis.Client) {
	q, jobID := "default", "j-dead-rd"
	client.HSet(ctx, "runiq:jobs", jobID, `{"job_id":"j-dead-rd"}`)
	client.LPush(ctx, "runiq:dead:"+q, jobID)
	client.ZAdd(ctx, "runiq:dead_ttl", redis.Z{
		Score:  float64(time.Now().Add(-2 * time.Hour).Unix()),
		Member: q + ":" + jobID,
	})
	if err := s.PurgeExpiredDLQ(ctx, 1*time.Hour); err != nil {
		t.Fatalf("purge failed: %v", err)
	}
	exist, _ := client.HExists(ctx, "runiq:jobs", jobID).Result()
	if exist {
		t.Errorf("expected dead job to be deleted from runiq:jobs")
	}
}

func TestDashboardBasicAuth(t *testing.T) {
	fakeStore := &FakeStorage{}
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok || user != "admin" || pass != "secret" {
				w.Header().Set("WWW-Authenticate", `Basic realm="Runiq Dashboard"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	server := queue.NewServer(fakeStore, ":8080", queue.WithMiddleware(mw))
	handler := server.Handler()
	assertBasicAuthHandler(t, handler)
}

func assertBasicAuthHandler(t *testing.T, handler http.Handler) {
	req := httptest.NewRequest("GET", "/api/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", w.Code)
	}
	req = httptest.NewRequest("GET", "/api/stats", nil)
	req.SetBasicAuth("admin", "secret")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", w.Code)
	}
}
