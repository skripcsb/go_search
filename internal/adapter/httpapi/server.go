package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"searchtrends/internal/application/trends"
)

type ReadyChecker interface {
	Ready() bool
}

type Options struct {
	Addr    string
	Service *trends.Service
	Metrics prometheus.Gatherer
	Logger  *log.Logger
	Ready   ReadyChecker
}

type Server struct {
	server *http.Server
	app    *trends.Service
	ready  ReadyChecker
}

type topItemDTO struct {
	Query string `json:"query"`
	Count int    `json:"count"`
}

type topResponseDTO struct {
	WindowSeconds int          `json:"window_seconds"`
	Limit         int          `json:"limit"`
	Items         []topItemDTO `json:"items"`
}

type stopWordDTO struct {
	Word string `json:"word"`
}

func NewServer(options Options) *Server {
	srv := &Server{app: options.Service, ready: options.Ready}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.healthz)
	mux.HandleFunc("/readyz", srv.readyz)
	mux.HandleFunc("/api/v1/top", srv.top)
	mux.HandleFunc("/api/v1/stop-list", srv.stopList)
	mux.HandleFunc("/api/v1/stop-list/", srv.stopListItem)
	mux.Handle("/metrics", promhttp.HandlerFor(options.Metrics, promhttp.HandlerOpts{}))
	srv.server = &http.Server{Addr: options.Addr, Handler: mux}
	return srv
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

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready == nil || s.ready.Ready() {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
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

	windowSeconds, entries := s.app.Top(limit)
	items := make([]topItemDTO, 0, len(entries))
	for _, entry := range entries {
		items = append(items, topItemDTO{Query: entry.Query, Count: entry.Count})
	}
	writeJSON(w, http.StatusOK, topResponseDTO{WindowSeconds: windowSeconds, Limit: limit, Items: items})
}

func (s *Server) stopList(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string][]string{"items": s.app.StopWords()})
	case http.MethodPost:
		var payload stopWordDTO
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if !s.app.AddStopWord(payload.Word) {
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
	if !s.app.DeleteStopWord(word) {
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
