package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	_ "github.com/glebarez/go-sqlite"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/wesleyskap/orkai-runiq/v2/queue"
)

type ShellJob struct{}

type ShellJobArgs struct {
	Command string `json:"command"`
}

func buildShellCmd(ctx context.Context, command string) *exec.Cmd {
	if runtime.GOOS == "windows" {
		return exec.CommandContext(ctx, "powershell", "-Command", command)
	}
	return exec.CommandContext(ctx, "sh", "-c", command)
}

func (j *ShellJob) Perform(ctx context.Context, args []byte) error {
	var jobArgs ShellJobArgs
	if err := json.Unmarshal(args, &jobArgs); err != nil {
		jobArgs.Command = string(args)
	}
	if jobArgs.Command == "" {
		return fmt.Errorf("empty command")
	}
	cmd := buildShellCmd(ctx, jobArgs.Command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("command failed: %w, output: %s", err, string(output))
	}
	log.Printf("[ShellJob] Output:\n%s", string(output))
	return nil
}

type WebhookJob struct{}

type WebhookJobArgs struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

func parseWebhookArgs(args []byte) (WebhookJobArgs, error) {
	var jobArgs WebhookJobArgs
	if err := json.Unmarshal(args, &jobArgs); err != nil {
		return jobArgs, fmt.Errorf("failed to parse webhook args: %w", err)
	}
	if jobArgs.URL == "" {
		return jobArgs, fmt.Errorf("empty webhook URL")
	}
	if jobArgs.Method == "" {
		jobArgs.Method = "POST"
	}
	return jobArgs, nil
}

func buildWebhookReq(ctx context.Context, args WebhookJobArgs) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, args.Method, args.URL, strings.NewReader(args.Body))
	if err != nil {
		return nil, err
	}
	for k, v := range args.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func (j *WebhookJob) Perform(ctx context.Context, args []byte) error {
	jobArgs, err := parseWebhookArgs(args)
	if err != nil {
		return err
	}
	req, err := buildWebhookReq(ctx, jobArgs)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}

func main() {
	port := flag.String("port", ":8080", "Dashboard port")
	dsn := flag.String("dsn", "", "Connection string for storage backend")
	driver := flag.String("driver", "", "Database driver (postgres, redis, sqlite)")
	queuesFlag := flag.String("queue", "", "Comma-separated list of queues to poll")
	concurrency := flag.Int("concurrency", 10, "Concurrency level for workers")

	flag.Parse()

	if *dsn == "" || *driver == "" || *queuesFlag == "" {
		log.Fatalf("missing required flags: --dsn, --driver and --queue are mandatory")
	}

	runCli(*port, *dsn, *driver, *queuesFlag, *concurrency)
}

func initStorage(driver, dsn string) (interface{}, error) {
	switch driver {
	case "postgres":
		return connectPostgres(dsn)
	case "redis":
		return connectRedis(dsn)
	case "sqlite":
		return connectSqlite(dsn)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
}

func runCli(port, dsn, driver, queuesFlag string, concurrency int) {
	queues := strings.Split(queuesFlag, ",")
	for i, q := range queues {
		queues[i] = strings.TrimSpace(q)
	}

	storage, err := initStorage(driver, dsn)
	if err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}

	startServices(port, storage, concurrency, queues)
}

func connectPostgres(dsn string) (*queue.PostgresStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return queue.NewPostgresStorage(db)
}

func connectSqlite(dsn string) (*queue.SqliteStorage, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	return queue.NewSqliteStorage(db)
}

func connectRedis(dsn string) (*queue.RedisStorage, error) {
	var opts *redis.Options
	var err error
	if strings.HasPrefix(dsn, "redis://") || strings.HasPrefix(dsn, "rediss://") {
		opts, err = redis.ParseURL(dsn)
		if err != nil {
			return nil, err
		}
	} else {
		opts = &redis.Options{Addr: dsn}
	}
	return queue.NewRedisStorage(redis.NewClient(opts))
}

func runWorkerPool(ctx context.Context, pool *queue.WorkerPool, queues []string) {
	log.Printf("Starting worker pool polling queues: %v...", queues)
	if err := pool.Start(ctx, queues...); err != nil {
		log.Printf("Worker pool stopped: %v", err)
	}
}

func runDashboardServer(srv *queue.Server) {
	log.Printf("Starting dashboard server...")
	if err := srv.Start(); err != nil && err != http.ErrServerClosed {
		log.Printf("Dashboard server error: %v", err)
	}
}

func startServices(port string, storage interface{}, concurrency int, queues []string) {
	poolStore, ok := storage.(queue.WorkerPoolStorage)
	if !ok {
		log.Fatalf("storage engine does not support worker pool operations")
	}
	serverStore, ok := storage.(queue.ServerStorage)
	if !ok {
		log.Fatalf("storage engine does not support dashboard server operations")
	}

	pool := queue.NewWorkerPool(poolStore, concurrency)
	pool.Register("shell", &ShellJob{})
	pool.Register("webhook", &WebhookJob{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runWorkerPool(ctx, pool, queues)

	srv := queue.NewServer(serverStore, port)
	go runDashboardServer(srv)

	handleShutdown(cancel, srv)
}

func handleShutdown(cancel context.CancelFunc, srv *queue.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down gracefully...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Dashboard server shutdown error: %v", err)
	}
	log.Println("Runiq stopped.")
}
