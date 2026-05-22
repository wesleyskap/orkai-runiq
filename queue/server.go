package queue

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"time"
)

//go:embed assets/*
var assetsFS embed.FS

// Server serves the dashboard UI and API.
type Server struct {
	storage     ServerStorage
	port        string
	httpServer  *http.Server
	middlewares []func(http.Handler) http.Handler
}

// ServerOption defines functional configuration options for Server.
type ServerOption func(*Server)

// WithMiddleware adds one or more HTTP middlewares to the dashboard server.
// Usage example:
//
//	server := queue.NewServer(storage, ":8080", queue.WithMiddleware(auth))
func WithMiddleware(mws ...func(http.Handler) http.Handler) ServerOption {
	return func(s *Server) {
		s.middlewares = append(s.middlewares, mws...)
	}
}

// NewServer instantiates a new Dashboard Server.
// Usage example:
//
//	server := queue.NewServer(storage, ":8080")
func NewServer(storage ServerStorage, port string, opts ...ServerOption) *Server {
	s := &Server{
		storage: storage,
		port:    port,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.httpServer = &http.Server{
		Addr:    port,
		Handler: s.Handler(),
	}
	return s
}

// Handler returns the HTTP router handler (useful for test assertions).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/jobs/retry", s.handleRetry)
	mux.HandleFunc("/api/jobs/cancel", s.handleCancel)
	mux.HandleFunc("/api/queues/clear", s.handleClearQueue)
	mux.HandleFunc("/api/queues/pause", s.handlePauseQueue)
	mux.HandleFunc("/api/queues/resume", s.handleResumeQueue)
	mux.HandleFunc("/api/jobs/detail", s.handleJobDetail)
	mux.HandleFunc("/api/jobs/failed/retry", s.handleRetryAllFailed)
	mux.HandleFunc("/api/jobs/failed/purge", s.handlePurgeAllFailed)
	mux.HandleFunc("/api/jobs", s.handleGetJobs)
	mux.HandleFunc("/api/stats/stream", s.handleStatsStream)
	mux.HandleFunc("/api/jobs/bulk-retry", s.handleBulkRetry)
	mux.HandleFunc("/api/jobs/bulk-cancel", s.handleBulkCancel)
	mux.HandleFunc("/api/jobs/bulk-purge", s.handleBulkPurge)
	mux.HandleFunc("/metrics", s.handleMetrics)

	sub, err := fs.Sub(assetsFS, "assets")
	if err == nil {
		mux.Handle("/", http.FileServer(http.FS(sub)))
	}
	return s.applyMiddlewares(mux)
}

func (s *Server) applyMiddlewares(handler http.Handler) http.Handler {
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		handler = s.middlewares[i](handler)
	}
	return handler
}

// Start runs the HTTP listener.
func (s *Server) Start() error {
	err := s.httpServer.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// StartTLS runs the HTTPS listener.
func (s *Server) StartTLS(certFile, keyFile string) error {
	err := s.httpServer.ListenAndServeTLS(certFile, keyFile)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Shutdown stops the listener.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.storage.GetStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	jobID := r.URL.Query().Get("id")
	if jobID == "" {
		http.Error(w, "Missing job id parameter", http.StatusBadRequest)
		return
	}
	if err := s.storage.Retry(r.Context(), jobID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	jobID := r.URL.Query().Get("id")
	if jobID == "" {
		http.Error(w, "Missing job id parameter", http.StatusBadRequest)
		return
	}
	if err := s.storage.Cancel(r.Context(), jobID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleClearQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	queueName := r.URL.Query().Get("name")
	if queueName == "" {
		http.Error(w, "Missing queue name parameter", http.StatusBadRequest)
		return
	}
	if err := s.storage.ClearQueue(r.Context(), queueName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handlePauseQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Missing queue name parameter", http.StatusBadRequest)
		return
	}
	if err := s.storage.PauseQueue(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleResumeQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Missing queue name parameter", http.StatusBadRequest)
		return
	}
	if err := s.storage.ResumeQueue(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleJobDetail(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("id")
	if jobID == "" {
		http.Error(w, "Missing job id parameter", http.StatusBadRequest)
		return
	}
	env, err := s.storage.GetJobDetail(r.Context(), jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if env == nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(env)
}

func (s *Server) handleRetryAllFailed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.storage.RetryAllFailed(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handlePurgeAllFailed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.storage.PurgeAllFailed(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type JobsResponse struct {
	Jobs  []JobDetail `json:"jobs"`
	Total int64       `json:"total"`
}

func (s *Server) handleGetJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	status := r.URL.Query().Get("status")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, total, err := s.storage.GetJobs(r.Context(), q, status, page, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(JobsResponse{Jobs: jobs, Total: total})
}

func (s *Server) handleStatsStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	for r.Context().Err() == nil && s.sendStreamStats(w, r.Context()) == nil {
		time.Sleep(time.Second)
	}
}

func (s *Server) sendStreamStats(w http.ResponseWriter, ctx context.Context) error {
	stats, err := s.storage.GetStats(ctx)
	if err != nil {
		return err
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

type BulkRequest struct {
	IDs []string `json:"ids"`
}

func decodeBulkIDs(r *http.Request) ([]string, error) {
	var req BulkRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req.IDs, err
}

func (s *Server) handleBulkRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	ids, err := decodeBulkIDs(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.storage.BulkRetry(r.Context(), ids); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBulkCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	ids, err := decodeBulkIDs(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.storage.BulkCancel(r.Context(), ids); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBulkPurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	ids, err := decodeBulkIDs(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.storage.BulkPurge(r.Context(), ids); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func writeQueueMetric(w io.Writer, metric, queue, status string, val int64) {
	_, err := fmt.Fprintf(w, "%s{queue=\"%s\", status=\"%s\"} %d\n", metric, queue, status, val)
	if err != nil {
		return
	}
}

func writeQueuePausedMetric(w io.Writer, queue string, paused bool) {
	val := 0
	if paused {
		val = 1
	}
	_, _ = fmt.Fprintf(w, "runiq_queue_paused{queue=\"%s\"} %d\n", queue, val)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats, err := s.storage.GetStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, q := range stats.Queues {
		writeQueueMetric(w, "runiq_jobs_count", q.Name, "pending", q.Pending)
		writeQueueMetric(w, "runiq_jobs_count", q.Name, "running", q.Running)
		writeQueueMetric(w, "runiq_jobs_count", q.Name, "failed", q.Failed)
		writeQueueMetric(w, "runiq_jobs_count", q.Name, "processed", q.Processed)
		writeQueuePausedMetric(w, q.Name, q.Paused)
	}
}

