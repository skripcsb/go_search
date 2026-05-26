package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"searchtrends/internal/application/trends"
	"searchtrends/internal/domain"
	"searchtrends/internal/infra/memorystore"
	"searchtrends/internal/metrics"
)

func TestHTTPTopIntegration(t *testing.T) {
	collector := metrics.New()
	st := memorystore.New(memorystore.Options{Window: 5 * time.Minute, MaxTop: 100, Metrics: collector})
	defer st.Close()
	svc := trends.NewService(st)

	srv := NewServer(Options{Addr: "127.0.0.1:18081", Service: svc, Metrics: collector.Registry(), Ready: nil})

	go func() {
		_ = srv.ListenAndServe()
	}()
	// give server a moment to start
	time.Sleep(50 * time.Millisecond)

	base := time.Now().UTC()
	_ = svc.Ingest(domain.SearchEvent{EventID: "e1", Query: "iphone 15", SessionID: "s1", OccurredAt: base})
	_ = svc.Ingest(domain.SearchEvent{EventID: "e2", Query: "iphone 15", SessionID: "s2", OccurredAt: base.Add(1 * time.Second)})
	_ = svc.Ingest(domain.SearchEvent{EventID: "e3", Query: "sneakers", SessionID: "s3", OccurredAt: base.Add(2 * time.Second)})

	// wait for snapshot rebuild
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://127.0.0.1:18081/api/v1/top?n=2")
		if err == nil && resp.StatusCode == http.StatusOK {
			var body struct {
				Items []struct {
					Query string `json:"query"`
					Count int    `json:"count"`
				} `json:"items"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if len(body.Items) >= 2 {
				if body.Items[0].Query == "iphone 15" && body.Items[0].Count == 2 {
					_ = srv.Shutdown(context.Background())
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = srv.Shutdown(context.Background())
	t.Fatal("top did not reflect ingested events in time")
}
