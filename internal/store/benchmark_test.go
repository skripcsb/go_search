package store

import (
	"fmt"
	"testing"
	"time"

	"searchtrends/internal/model"
)

func BenchmarkIngestAndTop(b *testing.B) {
	st := New(Options{Window: 5 * time.Minute, MaxTop: 1000, DedupWindow: 5 * time.Minute})
	defer st.Close()
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 1000; i++ {
		_ = st.AddEvent(model.SearchEvent{EventID: fmt.Sprintf("seed-%d", i), Query: fmt.Sprintf("query-%d", i%50), SessionID: fmt.Sprintf("session-%d", i%100), OccurredAt: base})
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(st.GetTop(10)) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = st.AddEvent(model.SearchEvent{EventID: fmt.Sprintf("evt-%d", i+1000), Query: fmt.Sprintf("query-%d", i%50), SessionID: fmt.Sprintf("session-%d", i%100), OccurredAt: base.Add(time.Duration(i) * time.Second)})
		_ = st.GetTop(10)
	}
}
