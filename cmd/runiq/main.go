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

type config struct {
	port        string
	dsn         string
	driver      string
	queues      string
	tlsCert     string
	tlsKey      string
	authUser    string
	authPass    string
	concurrency int
	workerOnly  bool
}

func main() {
	cfg := parseFlags()
	if cfg.dsn == "" || cfg.driver == "" || cfg.queues == "" {
		log.Fatalf("missing required flags: --dsn, --driver and --queue are mandatory")
	}
	runCli(cfg)
}

func parseFlags() config {
	cfg := config{}
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		cfg.workerOnly = true
		os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
	}
	flag.StringVar(&cfg.port, "port", ":8080", "Dashboard port")
	flag.StringVar(&cfg.dsn, "dsn", "", "Connection string for storage backend")
	flag.StringVar(&cfg.driver, "driver", "", "Database driver (postgres, redis, sqlite)")
	flag.StringVar(&cfg.queues, "queue", "", "Comma-separated list of queues to poll")
	flag.IntVar(&cfg.concurrency, "concurrency", 10, "Concurrency level for workers")
	flag.StringVar(&cfg.tlsCert, "tls-cert", "", "Path to TLS cert file")
	flag.StringVar(&cfg.tlsKey, "tls-key", "", "Path to TLS key file")
	flag.StringVar(&cfg.authUser, "basic-auth-user", "", "Basic auth username")
	flag.StringVar(&cfg.authPass, "basic-auth-pass", "", "Basic auth password")
	flag.Parse()
	return cfg
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

func runCli(cfg config) {
	queues := strings.Split(cfg.queues, ",")
	for i, q := range queues {
		queues[i] = strings.TrimSpace(q)
	}
	storage, err := initStorage(cfg.driver, cfg.dsn)
	if err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}
	startServices(cfg, storage, queues)
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

func basicAuth(username, password string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok || user != username || pass != password {
				w.Header().Set("WWW-Authenticate", `Basic realm="Runiq Dashboard"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func buildServer(cfg config, store queue.ServerStorage) *queue.Server {
	var opts []queue.ServerOption
	if cfg.authUser != "" && cfg.authPass != "" {
		opts = append(opts, queue.WithMiddleware(basicAuth(cfg.authUser, cfg.authPass)))
	}
	return queue.NewServer(store, cfg.port, opts...)
}

func runDashboardServer(srv *queue.Server, cfg config) {
	log.Printf("Starting dashboard server on %s...", cfg.port)
	var err error
	if cfg.tlsCert != "" && cfg.tlsKey != "" {
		err = srv.StartTLS(cfg.tlsCert, cfg.tlsKey)
	} else {
		err = srv.Start()
	}
	if err != nil && err != http.ErrServerClosed {
		log.Printf("Dashboard server error: %v", err)
	}
}

func createWorkerPool(store queue.WorkerPoolStorage, concurrency int) *queue.WorkerPool {
	pool := queue.NewWorkerPool(store, concurrency)
	pool.Register("shell", &ShellJob{})
	pool.Register("webhook", &WebhookJob{})
	return pool
}

func startDashboardIfEnabled(cfg config, storage interface{}) *queue.Server {
	if cfg.workerOnly {
		return nil
	}
	serverStore, _ := storage.(queue.ServerStorage)
	if serverStore == nil {
		log.Fatalf("storage engine does not support server operations")
	}
	srv := buildServer(cfg, serverStore)
	go runDashboardServer(srv, cfg)
	return srv
}

func startServices(cfg config, storage interface{}, queues []string) {
	poolStore, _ := storage.(queue.WorkerPoolStorage)
	if poolStore == nil {
		log.Fatalf("storage engine does not support worker pool operations")
	}
	pool := createWorkerPool(poolStore, cfg.concurrency)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runWorkerPool(ctx, pool, queues)
	srv := startDashboardIfEnabled(cfg, storage)
	handleShutdown(cancel, srv)
}

func shutdownServer(srv *queue.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Dashboard server shutdown error: %v", err)
	}
}

func handleShutdown(cancel context.CancelFunc, srv *queue.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("Shutting down gracefully...")
	cancel()
	shutdownServer(srv)
	log.Println("Runiq stopped.")
}
