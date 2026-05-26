package memorystore

import (
	"container/heap"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"searchtrends/internal/domain"
	"searchtrends/internal/metrics"
)

const (
	defaultBucketCount       = 300
	defaultMaxQueryPerBucket = 1000
)

type Options struct {
	Window            time.Duration
	MaxTop            int
	DedupWindow       time.Duration
	MaxQueryPerBucket int
	Metrics           *metrics.Collector
	StopListFile      string
}

type bucket struct {
	second int64
	counts map[string]int
}

type Store struct {
	mu                sync.RWMutex
	window            time.Duration
	maxTop            int
	dedupWindow       time.Duration
	maxQueryPerBucket int
	metrics           *metrics.Collector
	bucketCount       int
	buckets           []bucket
	currentSecond     int64
	aggregate         map[string]int
	stopList          map[string]struct{}
	stopListFile      string
	seenEvents        map[string]int64
	seenSessionQuery  map[string]int64
	snapshot          atomic.Value
	recomputeCh       chan struct{}
	shutdown          chan struct{}
	startOnce         sync.Once
	closeOnce         sync.Once
}

func New(options Options) *Store {
	window := options.Window
	if window <= 0 {
		window = 5 * time.Minute
	}
	maxTop := options.MaxTop
	if maxTop <= 0 {
		maxTop = 1000
	}
	dedupWindow := options.DedupWindow
	if dedupWindow <= 0 {
		dedupWindow = window
	}
	st := &Store{
		window:            window,
		maxTop:            maxTop,
		dedupWindow:       dedupWindow,
		maxQueryPerBucket: options.MaxQueryPerBucket,
		metrics:           options.Metrics,
		bucketCount:       int(window.Seconds()),
		aggregate:         make(map[string]int),
		stopList:          make(map[string]struct{}),
		stopListFile:      options.StopListFile,
		seenEvents:        make(map[string]int64),
		seenSessionQuery:  make(map[string]int64),
		recomputeCh:       make(chan struct{}, 1),
		shutdown:          make(chan struct{}),
	}
	if st.bucketCount <= 0 {
		st.bucketCount = defaultBucketCount
	}
	if st.maxQueryPerBucket <= 0 {
		st.maxQueryPerBucket = defaultMaxQueryPerBucket
	}
	// load stop-list from file if provided
	if st.stopListFile != "" {
		if data, err := os.ReadFile(st.stopListFile); err == nil {
			var words []string
			if err := json.Unmarshal(data, &words); err == nil {
				for _, w := range words {
					st.stopList[w] = struct{}{}
				}
			}
		}
	}
	st.buckets = make([]bucket, st.bucketCount)
	st.snapshot.Store([]domain.TrendEntry{})
	st.start()
	return st
}

func (s *Store) AddEvent(event domain.SearchEvent) domain.IngestResult {
	normalizedQuery, validationReason := s.validateEvent(event)
	if validationReason != "" {
		s.observeIgnored(validationReason)
		return domain.IngestResult{Reason: validationReason}
	}

	seconds := event.OccurredAt.UTC().Unix()
	querySessionKey := event.SessionID + "\x00" + normalizedQuery

	s.mu.Lock()
	defer s.mu.Unlock()

	if reason := s.processLocked(event, normalizedQuery, querySessionKey, seconds); reason != "" {
		s.observeIgnored(reason)
		return domain.IngestResult{Reason: reason}
	}

	s.observeAccepted()
	s.signalRecompute()
	return domain.IngestResult{Accepted: true}
}

func (s *Store) GetTop(limit int) []domain.TrendEntry {
	limit = s.normalizeLimit(limit)
	items, _ := s.snapshot.Load().([]domain.TrendEntry)
	if len(items) < limit {
		limit = len(items)
	}
	result := make([]domain.TrendEntry, limit)
	copy(result, items[:limit])
	return result
}

func (s *Store) WindowSeconds() int {
	return int(s.window.Seconds())
}

func (s *Store) AddStopWord(word string) bool {
	normalized, err := domain.NormalizeQuery(word)
	if err != nil {
		return false
	}
	s.mu.Lock()
	_, existed := s.stopList[normalized]
	s.stopList[normalized] = struct{}{}
	s.mu.Unlock()
	// persist stop-list
	_ = s.saveStopList()
	s.recomputeSnapshot()
	return !existed
}

func (s *Store) DeleteStopWord(word string) bool {
	normalized, err := domain.NormalizeQuery(word)
	if err != nil {
		return false
	}
	s.mu.Lock()
	_, existed := s.stopList[normalized]
	delete(s.stopList, normalized)
	s.mu.Unlock()
	_ = s.saveStopList()
	s.recomputeSnapshot()
	return existed
}

func (s *Store) saveStopList() error {
	if s.stopListFile == "" {
		return nil
	}
	s.mu.RLock()
	words := make([]string, 0, len(s.stopList))
	for w := range s.stopList {
		words = append(words, w)
	}
	s.mu.RUnlock()
	data, err := json.MarshalIndent(words, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.stopListFile, data, 0644)
}

func (s *Store) StopWords() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	words := make([]string, 0, len(s.stopList))
	for word := range s.stopList {
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

func (s *Store) Close() {
	s.closeOnce.Do(func() {
		close(s.shutdown)
	})
}

func (s *Store) validateEvent(event domain.SearchEvent) (string, string) {
	normalizedQuery, err := domain.NormalizeQuery(event.Query)
	if err != nil {
		return "", "invalid_query"
	}
	if event.EventID == "" || event.SessionID == "" || event.OccurredAt.IsZero() {
		return "", "invalid_event"
	}
	return normalizedQuery, ""
}

func (s *Store) processLocked(event domain.SearchEvent, normalizedQuery, querySessionKey string, seconds int64) string {
	s.ensureCurrentSecondLocked(seconds)
	s.advanceAndPruneLocked(seconds)

	if s.isExpiredEventLocked(seconds) {
		return "expired_event"
	}
	if s.isStopListedLocked(normalizedQuery) {
		return "stoplist"
	}
	if s.isDuplicateEventLocked(event.EventID) {
		s.observeDuplicate("duplicate_event")
		return "duplicate_event"
	}
	if s.isDuplicateSessionQueryLocked(querySessionKey) {
		s.observeDuplicate("duplicate_session_query")
		return "duplicate_session_query"
	}

	// enforce per-bucket rate limiting for the same query
	if s.maxQueryPerBucket > 0 {
		bucket := s.bucketForSecondLocked(seconds)
		if bucket.counts[normalizedQuery] >= s.maxQueryPerBucket {
			return "rate_limited"
		}
	}

	s.recordEventLocked(event.EventID, querySessionKey, normalizedQuery, seconds)
	return ""
}

func (s *Store) ensureCurrentSecondLocked(seconds int64) {
	if s.currentSecond == 0 {
		s.currentSecond = seconds
	}
}

func (s *Store) advanceAndPruneLocked(seconds int64) {
	if seconds > s.currentSecond {
		s.advanceLocked(seconds)
		return
	}
	s.pruneSeenLocked(s.currentSecond)
}

func (s *Store) isExpiredEventLocked(seconds int64) bool {
	return seconds < s.currentSecond-int64(s.bucketCount-1)
}

func (s *Store) isStopListedLocked(query string) bool {
	_, exists := s.stopList[query]
	return exists
}

func (s *Store) isDuplicateEventLocked(eventID string) bool {
	_, exists := s.seenEvents[eventID]
	return exists
}

func (s *Store) isDuplicateSessionQueryLocked(key string) bool {
	_, exists := s.seenSessionQuery[key]
	return exists
}

func (s *Store) recordEventLocked(eventID, querySessionKey, query string, seconds int64) {
	bucket := s.bucketForSecondLocked(seconds)
	bucket.counts[query]++
	s.aggregate[query]++
	s.seenEvents[eventID] = seconds
	s.seenSessionQuery[querySessionKey] = seconds
}

func (s *Store) normalizeLimit(limit int) int {
	if limit <= 0 {
		return 10
	}
	if limit > s.maxTop {
		return s.maxTop
	}
	return limit
}

func (s *Store) start() {
	s.startOnce.Do(func() {
		go s.recomputeLoop()
		go s.expireLoop()
	})
}

func (s *Store) recomputeLoop() {
	for {
		select {
		case <-s.shutdown:
			return
		case <-s.recomputeCh:
			s.recomputeSnapshot()
			s.drainRecomputeSignals()
		}
	}
}

func (s *Store) expireLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdown:
			return
		case tick := <-ticker.C:
			s.mu.Lock()
			if s.currentSecond == 0 {
				s.currentSecond = tick.UTC().Unix()
			} else if tick.UTC().Unix() > s.currentSecond {
				s.advanceLocked(tick.UTC().Unix())
			}
			s.mu.Unlock()
			s.signalRecompute()
		}
	}
}

func (s *Store) advanceLocked(targetSecond int64) {
	for s.currentSecond < targetSecond {
		s.currentSecond++
		idx := int(s.currentSecond % int64(s.bucketCount))
		bucket := &s.buckets[idx]
		if bucket.second != s.currentSecond {
			s.dropBucketFromAggregateLocked(bucket)
			if bucket.counts != nil {
				clear(bucket.counts)
			}
			bucket.second = s.currentSecond
		}
	}
	s.pruneSeenLocked(targetSecond)
}

func (s *Store) bucketForSecondLocked(second int64) *bucket {
	idx := int(second % int64(s.bucketCount))
	bucket := &s.buckets[idx]
	if bucket.second != second {
		s.dropBucketFromAggregateLocked(bucket)
		if bucket.counts != nil {
			clear(bucket.counts)
		}
		bucket.second = second
	}
	if bucket.counts == nil {
		bucket.counts = make(map[string]int)
	}
	return bucket
}

func (s *Store) dropBucketFromAggregateLocked(bucket *bucket) {
	if bucket.second == 0 || bucket.counts == nil {
		return
	}
	for query, count := range bucket.counts {
		s.aggregate[query] -= count
		if s.aggregate[query] <= 0 {
			delete(s.aggregate, query)
		}
	}
}

func (s *Store) pruneSeenLocked(nowSecond int64) {
	cutoff := nowSecond - int64(s.dedupWindow.Seconds())
	for key, seenAt := range s.seenEvents {
		if seenAt <= cutoff {
			delete(s.seenEvents, key)
		}
	}
	for key, seenAt := range s.seenSessionQuery {
		if seenAt <= cutoff {
			delete(s.seenSessionQuery, key)
		}
	}
}

func (s *Store) recomputeSnapshot() {
	// build a bounded min-heap of size up to s.maxTop to avoid O(N log N) full sort
	s.mu.RLock()
	h := &minHeap{}
	heap.Init(h)
	for query, count := range s.aggregate {
		if _, blocked := s.stopList[query]; blocked {
			continue
		}
		heap.Push(h, domain.TrendEntry{Query: query, Count: count})
		if h.Len() > s.maxTop {
			heap.Pop(h)
		}
	}
	stopListSize := len(s.stopList)
	s.mu.RUnlock()

	// pop heap into slice in descending order
	n := h.Len()
	items := make([]domain.TrendEntry, n)
	for i := n - 1; i >= 0; i-- {
		items[i] = heap.Pop(h).(domain.TrendEntry)
	}
	s.snapshot.Store(items)
	if s.metrics != nil {
		s.metrics.TopItems.Set(float64(len(items)))
		s.metrics.StopListItems.Set(float64(stopListSize))
	}
}

// minHeap is a min-heap by Count, tie-breaker by Query (reverse for deterministic pop)
type minHeap []domain.TrendEntry

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(i, j int) bool {
	if h[i].Count != h[j].Count {
		return h[i].Count < h[j].Count
	}
	// when counts equal, consider lexicographically larger queries as "smaller"
	// so that lexicographically smaller queries survive in the heap when bounded
	return h[i].Query > h[j].Query
}
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)   { *h = append(*h, x.(domain.TrendEntry)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *Store) signalRecompute() {
	select {
	case s.recomputeCh <- struct{}{}:
	default:
	}
}

func (s *Store) drainRecomputeSignals() {
	for {
		select {
		case <-s.recomputeCh:
		default:
			return
		}
	}
}

func (s *Store) observeAccepted() {
	if s.metrics != nil {
		s.metrics.EventsIngested.Inc()
	}
}

func (s *Store) observeIgnored(reason string) {
	if s.metrics != nil {
		s.metrics.EventsIgnored.WithLabelValues(reason).Inc()
	}
}

func (s *Store) observeDuplicate(reason string) {
	if s.metrics != nil {
		s.metrics.DuplicatesDropped.Inc()
		s.metrics.EventsIgnored.WithLabelValues(reason).Inc()
	}
}
