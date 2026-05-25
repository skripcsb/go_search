package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"searchtrends/internal/model"
	"searchtrends/internal/store"
)

type Options struct {
	Addr    string
	Store   *store.Store
	Metrics prometheus.Gatherer
	Logger  *log.Logger
	MaxTop  int
	Window  time.Duration
}

type Server struct {
	server *http.Server
	store  *store.Store
	window time.Duration
	maxTop int
	logger *log.Logger
}

func NewServer(options Options) *Server {
	server := &Server{store: options.Store, window: options.Window, maxTop: options.MaxTop, logger: options.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.healthz)
	mux.HandleFunc("/readyz", server.readyz)
	mux.HandleFunc("/api/v1/top", server.top)
	mux.HandleFunc("/api/v1/stop-list", server.stopList)
	mux.HandleFunc("/api/v1/stop-list/", server.stopListItem)
	mux.Handle("/metrics", promhttp.HandlerFor(options.Metrics, promhttp.HandlerOpts{}))
	server.server = &http.Server{Addr: options.Addr, Handler: mux}
	return server
}

func (s *Server) Addr() string {
	return s.server.Addr
}

func (s *Server) ListenAndServe() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) top(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	limit := 10
	if raw := r.URL.Query().Get("n"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			http.Error(w, "n must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	items := s.store.GetTop(limit)
	writeJSON(w, http.StatusOK, model.TopResponse{
		WindowSeconds: s.store.WindowSeconds(),
		Limit:         limit,
		Items:         items,
	})
}

func (s *Server) stopList(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string][]string{"items": s.store.StopWords()})
	case http.MethodPost:
		var payload model.StopWord
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if !s.store.AddStopWord(payload.Word) {
			http.Error(w, "invalid stop word", http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusCreated, payload)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) stopListItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	word := strings.TrimPrefix(r.URL.Path, "/api/v1/stop-list/")
	if word == "" {
		http.Error(w, "word is required", http.StatusBadRequest)
		return
	}
	if !s.store.DeleteStopWord(word) {
		http.Error(w, "invalid stop word", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}
