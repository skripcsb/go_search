package store

import (
	"testing"
	"time"

	"searchtrends/internal/model"
)

func TestStoreTopAndDedup(t *testing.T) {
	st := New(Options{Window: 5 * time.Minute, MaxTop: 100, DedupWindow: 5 * time.Minute})
	t.Cleanup(st.Close)
	base := time.Unix(1_700_000_000, 0).UTC()

	accepted := st.AddEvent(model.SearchEvent{EventID: "1", Query: "  Air Fryer  ", SessionID: "s1", OccurredAt: base})
	if !accepted.Accepted {
		t.Fatalf("expected first event to be accepted, got %+v", accepted)
	}
	duplicate := st.AddEvent(model.SearchEvent{EventID: "2", Query: "air fryer", SessionID: "s1", OccurredAt: base.Add(10 * time.Second)})
	if duplicate.Accepted {
		t.Fatalf("expected duplicate session/query to be ignored")
	}
	second := st.AddEvent(model.SearchEvent{EventID: "3", Query: "sneakers", SessionID: "s2", OccurredAt: base.Add(20 * time.Second)})
	if !second.Accepted {
		t.Fatalf("expected second query to be accepted")
	}
	waitForTop(t, st, 2)
	top := st.GetTop(10)
	if top[0].Query != "air fryer" || top[0].Count != 1 {
		t.Fatalf("unexpected top item: %+v", top[0])
	}
}

func TestStopListHidesQuery(t *testing.T) {
	st := New(Options{Window: 5 * time.Minute, MaxTop: 100, DedupWindow: 5 * time.Minute})
	t.Cleanup(st.Close)
	base := time.Unix(1_700_000_000, 0).UTC()
	_ = st.AddEvent(model.SearchEvent{EventID: "1", Query: "backpack", SessionID: "s1", OccurredAt: base})
	waitForTop(t, st, 1)
	if !st.AddStopWord("backpack") {
		t.Fatalf("expected stop word to be added")
	}
	if top := st.GetTop(10); len(top) != 0 {
		t.Fatalf("expected stoplisted query to disappear, got %+v", top)
	}
}

func TestExpiration(t *testing.T) {
	st := New(Options{Window: 5 * time.Minute, MaxTop: 100, DedupWindow: 5 * time.Minute})
	t.Cleanup(st.Close)
	base := time.Unix(1_700_000_000, 0).UTC()
	_ = st.AddEvent(model.SearchEvent{EventID: "1", Query: "laptop", SessionID: "s1", OccurredAt: base})
	_ = st.AddEvent(model.SearchEvent{EventID: "2", Query: "laptop", SessionID: "s2", OccurredAt: base.Add(301 * time.Second)})
	waitForTop(t, st, 1)
	items := st.GetTop(10)
	if items[0].Count != 1 {
		t.Fatalf("expected expired count to be removed, got %+v", items[0])
	}
}

func waitForTop(t *testing.T, st *Store, expected int) {
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