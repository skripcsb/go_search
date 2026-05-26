package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Collector struct {
	EventsIngested     prometheus.Counter
	EventsIgnored      *prometheus.CounterVec
	TopItems           prometheus.Gauge
	StopListItems      prometheus.Gauge
	DuplicatesDropped  prometheus.Counter
	ConsumerLagSeconds prometheus.Gauge
	IngestLatency      prometheus.Histogram
	TopReadLatency     prometheus.Histogram
	registry           *prometheus.Registry
}

func New() *Collector {
	registry := prometheus.NewRegistry()
	c := &Collector{
		EventsIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "searchtrends_events_ingested_total",
			Help: "Total accepted search events.",
		}),
		EventsIgnored: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "searchtrends_events_ignored_total",
			Help: "Total ignored search events grouped by reason.",
		}, []string{"reason"}),
		TopItems: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "searchtrends_top_items",
			Help: "Number of items currently in the cached top snapshot.",
		}),
		StopListItems: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "searchtrends_stoplist_items",
			Help: "Number of stop-list entries.",
		}),
		DuplicatesDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "searchtrends_duplicates_dropped_total",
			Help: "Total deduplicated events.",
		}),
		ConsumerLagSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "searchtrends_consumer_lag_seconds",
			Help: "Lag between event time and ingest time in seconds.",
		}),
		IngestLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "searchtrends_ingest_latency_seconds",
			Help:    "Ingest processing time in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
		}),
		TopReadLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "searchtrends_top_read_latency_seconds",
			Help:    "Top API read latency in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.00005, 2, 12),
		}),
		registry: registry,
	}
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(c.EventsIngested)
	registry.MustRegister(c.EventsIgnored)
	registry.MustRegister(c.TopItems)
	registry.MustRegister(c.StopListItems)
	registry.MustRegister(c.DuplicatesDropped)
	registry.MustRegister(c.ConsumerLagSeconds)
	registry.MustRegister(c.IngestLatency)
	registry.MustRegister(c.TopReadLatency)
	return c
}

func (c *Collector) Registry() *prometheus.Registry {
	return c.registry
}

func (c *Collector) ObserveConsumerLag(occurredAt time.Time) {
	if occurredAt.IsZero() {
		return
	}
	lag := time.Since(occurredAt).Seconds()
	if lag < 0 {
		lag = 0
	}
	c.ConsumerLagSeconds.Set(lag)
}
