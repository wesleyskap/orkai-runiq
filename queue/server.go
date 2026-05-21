package queue

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
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
//	server := queue.NewServer(storage, ":8080", queue.WithMiddleware(auth))
func WithMiddleware(mws ...func(http.Handler) http.Handler) ServerOption {
	return func(s *Server) {
		s.middlewares = append(s.middlewares, mws...)
	}
}

// NewServer instantiates a new Dashboard Server.
// Usage example:
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

// Shutdown stops the listener gracefully.
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
