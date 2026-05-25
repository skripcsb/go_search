package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"searchtrends/internal/model"
	"searchtrends/internal/store"
)

func TestTopHandler(t *testing.T) {
	st := store.New(store.Options{Window: 5 * time.Minute, MaxTop: 100})
	t.Cleanup(st.Close)
	_ = st.AddEvent(model.SearchEvent{EventID: "1", Query: "iphone 15", SessionID: "s1", OccurredAt: time.Unix(1_700_000_000, 0).UTC()})
	waitForTop(t, st, 1)

	server := NewServer(Options{Addr: ":0", Store: st, Window: 5 * time.Minute})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/top?n=1", nil)
	rec := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	var response model.TopResponse
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 || response.Items[0].Query != "iphone 15" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func waitForTop(t *testing.T, st *store.Store, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(st.GetTop(10)) == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for top size %d", expected)
}