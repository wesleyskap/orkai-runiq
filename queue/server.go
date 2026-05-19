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
	storage    Storage
	port       string
	httpServer *http.Server
}

// NewServer instantiates a new Dashboard Server.
// Usage example:
//	server := queue.NewServer(storage, ":8080")
func NewServer(storage Storage, port string) *Server {
	s := &Server{
		storage: storage,
		port:    port,
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

	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		return mux
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))
	return mux
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
