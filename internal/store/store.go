package store

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"searchtrends/internal/metrics"
	"searchtrends/internal/model"
)

const bucketCount = 300

type Options struct {
	Window            time.Duration
	MaxTop            int
	RecomputeInterval time.Duration
	Metrics           *metrics.Collector
	DedupWindow       time.Duration
}

type bucket struct {
	second int64
	counts map[string]int
}

type Store struct {
	mu               sync.RWMutex
	window           time.Duration
	maxTop           int
	dedupWindow      time.Duration
	metrics          *metrics.Collector
	buckets          [bucketCount]bucket
	currentSecond    int64
	aggregate        map[string]int
	stopList         map[string]struct{}
	seenEvents       map[string]int64
	seenSessionQuery map[string]int64
	snapshot         atomic.Value
	recomputeCh      chan struct{}
	shutdown         chan struct{}
	startOnce        sync.Once
	closeOnce        sync.Once
	started          atomic.Bool
}

type IngestResult struct {
	Accepted bool
	Reason   string
}

func New(options Options) *Store {
	if options.Window <= 0 {
		options.Window = 5 * time.Minute
	}
	if options.MaxTop <= 0 {
		options.MaxTop = 1000
	}
	if options.DedupWindow <= 0 {
		options.DedupWindow = options.Window
	}
	store := &Store{
		window:           options.Window,
		maxTop:           options.MaxTop,
		dedupWindow:      options.DedupWindow,
		metrics:          options.Metrics,
		aggregate:        make(map[string]int),
		stopList:         make(map[string]struct{}),
		seenEvents:       make(map[string]int64),
		seenSessionQuery: make(map[string]int64),
		recomputeCh:      make(chan struct{}, 1),
		shutdown:         make(chan struct{}),
	}
	store.snapshot.Store([]model.TopItem{})
	store.Start()
	return store
}

func (s *Store) Start() {
	s.startOnce.Do(func() {
		s.started.Store(true)
		go s.recomputeLoop()
		go s.expireLoop()
	})
}

func (s *Store) Close() {
	s.closeOnce.Do(func() {
		close(s.shutdown)
	})
}

func (s *Store) AddEvent(event model.SearchEvent) IngestResult {
	normalizedQuery, err := model.NormalizeQuery(event.Query)
	if err != nil {
		if s.metrics != nil {
			s.metrics.EventsIgnored.WithLabelValues("invalid_query").Inc()
		}
		return IngestResult{Reason: "invalid_query"}
	}
	if event.EventID == "" || event.SessionID == "" || event.OccurredAt.IsZero() {
		if s.metrics != nil {
			s.metrics.EventsIgnored.WithLabelValues("invalid_event").Inc()
		}
		return IngestResult{Reason: "invalid_event"}
	}

	seconds := event.OccurredAt.UTC().Unix()
	querySessionKey := event.SessionID + "\x00" + normalizedQuery

	s.mu.Lock()
	if s.currentSecond == 0 {
		s.currentSecond = seconds
	}
	if seconds > s.currentSecond {
		s.advanceLocked(seconds)
	} else {
		s.pruneSeenLocked(s.currentSecond)
	}
	if seconds < s.currentSecond-int64(bucketCount-1) {
		s.mu.Unlock()
		if s.metrics != nil {
			s.metrics.EventsIgnored.WithLabelValues("expired_event").Inc()
		}
		return IngestResult{Reason: "expired_event"}
	}
	if _, blocked := s.stopList[normalizedQuery]; blocked {
		s.mu.Unlock()
		if s.metrics != nil {
			s.metrics.EventsIgnored.WithLabelValues("stoplist").Inc()
		}
		return IngestResult{Reason: "stoplist"}
	}
	if _, duplicate := s.seenEvents[event.EventID]; duplicate {
		s.mu.Unlock()
		if s.metrics != nil {
			s.metrics.DuplicatesDropped.Inc()
			s.metrics.EventsIgnored.WithLabelValues("duplicate_event").Inc()
		}
		return IngestResult{Reason: "duplicate_event"}
	}
	if _, duplicate := s.seenSessionQuery[querySessionKey]; duplicate {
		s.mu.Unlock()
		if s.metrics != nil {
			s.metrics.DuplicatesDropped.Inc()
			s.metrics.EventsIgnored.WithLabelValues("duplicate_session_query").Inc()
		}
		return IngestResult{Reason: "duplicate_session_query"}
	}

	bucket := s.bucketForSecondLocked(seconds)
	bucket.counts[normalizedQuery]++
	s.aggregate[normalizedQuery]++
	s.seenEvents[event.EventID] = seconds
	s.seenSessionQuery[querySessionKey] = seconds
	s.mu.Unlock()

	s.signalRecompute()
	if s.metrics != nil {
		s.metrics.EventsIngested.Inc()
	}
	return IngestResult{Accepted: true}
}

func (s *Store) AddStopWord(word string) bool {
	normalized, err := model.NormalizeQuery(word)
	if err != nil {
		return false
	}
	s.mu.Lock()
	_, existed := s.stopList[normalized]
	s.stopList[normalized] = struct{}{}
	s.mu.Unlock()
	s.recomputeSnapshot()
	return !existed
}

func (s *Store) DeleteStopWord(word string) bool {
	normalized, err := model.NormalizeQuery(word)
	if err != nil {
		return false
	}
	s.mu.Lock()
	_, existed := s.stopList[normalized]
	delete(s.stopList, normalized)
	s.mu.Unlock()
	s.recomputeSnapshot()
	return existed
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

func (s *Store) GetTop(limit int) []model.TopItem {
	if limit <= 0 {
		limit = 10
	}
	if limit > s.maxTop {
		limit = s.maxTop
	}
	items, _ := s.snapshot.Load().([]model.TopItem)
	if len(items) < limit {
		limit = len(items)
	}
	result := make([]model.TopItem, limit)
	copy(result, items[:limit])
	return result
}

func (s *Store) WindowSeconds() int {
	return int(s.window.Seconds())
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
		idx := int(s.currentSecond % bucketCount)
		bucket := &s.buckets[idx]
		if bucket.second != s.currentSecond {
			if bucket.second != 0 {
				for query, count := range bucket.counts {
					s.aggregate[query] -= count
					if s.aggregate[query] <= 0 {
						delete(s.aggregate, query)
					}
				}
			}
			clear(bucket.counts)
			bucket.second = s.currentSecond
		}
	}
	s.pruneSeenLocked(targetSecond)
}

func (s *Store) bucketForSecondLocked(second int64) *bucket {
	idx := int(second % bucketCount)
	bucket := &s.buckets[idx]
	if bucket.second != second {
		if bucket.second != 0 {
			for query, count := range bucket.counts {
				s.aggregate[query] -= count
				if s.aggregate[query] <= 0 {
					delete(s.aggregate, query)
				}
			}
		}
		clear(bucket.counts)
		bucket.second = second
	}
	if bucket.counts == nil {
		bucket.counts = make(map[string]int)
	}
	return bucket
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
	s.mu.RLock()
	items := make([]model.TopItem, 0, len(s.aggregate))
	for query, count := range s.aggregate {
		if _, blocked := s.stopList[query]; blocked {
			continue
		}
		items = append(items, model.TopItem{Query: query, Count: count})
	}
	stopListSize := len(s.stopList)
	s.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Query < items[j].Query
	})
	s.snapshot.Store(items)
	if s.metrics != nil {
		s.metrics.TopItems.Set(float64(len(items)))
		s.metrics.StopListItems.Set(float64(stopListSize))
	}
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
