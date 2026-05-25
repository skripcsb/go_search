package metrics

import "github.com/prometheus/client_golang/prometheus"

type Collector struct {
	EventsIngested    prometheus.Counter
	EventsIgnored     *prometheus.CounterVec
	TopItems          prometheus.Gauge
	StopListItems     prometheus.Gauge
	DuplicatesDropped prometheus.Counter
	registry          *prometheus.Registry
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
		registry: registry,
	}
	registry.MustRegister(c.EventsIngested)
	registry.MustRegister(c.EventsIgnored)
	registry.MustRegister(c.TopItems)
	registry.MustRegister(c.StopListItems)
	registry.MustRegister(c.DuplicatesDropped)
	return c
}

func (c *Collector) Registry() *prometheus.Registry {
	return c.registry
}
