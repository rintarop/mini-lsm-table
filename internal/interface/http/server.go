package http

import (
	"log"
	"net/http"
	"time"
)

// Server represents the HTTP server
type Server struct {
	handler *Handler
	server  *http.Server
}

// NewServer creates a new HTTP server
func NewServer(handler *Handler, port string) *Server {
	mux := http.NewServeMux()

	// Setup routes with logging middleware
	mux.HandleFunc("/api/put", loggingMiddleware(handler.HandlePut))
	mux.HandleFunc("/api/get/", loggingMiddleware(handler.HandleGet))
	mux.HandleFunc("/api/delete", loggingMiddleware(handler.HandleDelete))
	mux.HandleFunc("/api/status", loggingMiddleware(handler.HandleStatus))
	mux.HandleFunc("/api/recovery", loggingMiddleware(handler.HandleRecovery))
	mux.HandleFunc("/health", loggingMiddleware(handler.HandleHealth))
	mux.HandleFunc("/", loggingMiddleware(handler.HandleAPIDoc))

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{
		handler: handler,
		server:  server,
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	log.Printf("Server starting on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

// Middleware for logging HTTP requests
func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Create a custom ResponseWriter to capture status code
		lw := &loggingResponseWriter{ResponseWriter: w, statusCode: 200}

		next.ServeHTTP(lw, r)

		duration := time.Since(start)
		log.Printf("%s %s %d %v", r.Method, r.URL.Path, lw.statusCode, duration)
	})
}

// loggingResponseWriter wraps http.ResponseWriter to capture status code
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lw *loggingResponseWriter) WriteHeader(code int) {
	lw.statusCode = code
	lw.ResponseWriter.WriteHeader(code)
}
