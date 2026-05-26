package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"searchtrends/internal/application/trends"
	"searchtrends/internal/domain"
	"searchtrends/internal/infra/memorystore"
)

type readyStub struct {
	ready bool
}

func (r readyStub) Ready() bool {
	return r.ready
}

func TestTopHandler(t *testing.T) {
	store := memorystore.New(memorystore.Options{Window: 5 * time.Minute, MaxTop: 100})
	t.Cleanup(store.Close)
	app := trends.NewService(store)
	_ = app.Ingest(domain.SearchEvent{EventID: "1", Query: "iphone 15", SessionID: "s1", OccurredAt: time.Unix(1_700_000_000, 0).UTC()})
	waitForTop(t, app, 1)

	server := NewServer(Options{Addr: ":0", Service: app, Ready: readyStub{ready: true}})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/top?n=1", nil)
	rec := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	var response topResponseDTO
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 || response.Items[0].Query != "iphone 15" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestReadyzReflectsConsumerState(t *testing.T) {
	store := memorystore.New(memorystore.Options{})
	t.Cleanup(store.Close)
	app := trends.NewService(store)

	notReadyServer := NewServer(Options{Addr: ":0", Service: app, Ready: readyStub{ready: false}})
	notReadyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	notReadyRec := httptest.NewRecorder()
	notReadyServer.server.Handler.ServeHTTP(notReadyRec, notReadyReq)
	if notReadyRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when consumer is not ready, got %d", notReadyRec.Code)
	}

	readyServer := NewServer(Options{Addr: ":0", Service: app, Ready: readyStub{ready: true}})
	readyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyRec := httptest.NewRecorder()
	readyServer.server.Handler.ServeHTTP(readyRec, readyReq)
	if readyRec.Code != http.StatusOK {
		t.Fatalf("expected 200 when consumer is ready, got %d", readyRec.Code)
	}
}

func waitForTop(t *testing.T, app *trends.Service, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, top := app.Top(10)
		if len(top) == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for top size %d", expected)
}
