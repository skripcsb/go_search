package kafka

import "sync/atomic"

type Status struct {
	ready atomic.Bool
}

func (s *Status) SetReady(value bool) {
	s.ready.Store(value)
}

func (s *Status) Ready() bool {
	return s.ready.Load()
}
