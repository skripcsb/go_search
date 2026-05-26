package domain

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrEmptyQuery   = errors.New("empty query")
	ErrInvalidEvent = errors.New("invalid event")
)

type SearchEvent struct {
	EventID    string    `json:"event_id"`
	Query      string    `json:"query"`
	SessionID  string    `json:"session_id"`
	UserID     string    `json:"user_id,omitempty"`
	Source     string    `json:"source,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

type TrendEntry struct {
	Query string
	Count int
}

type IngestResult struct {
	Accepted bool
	Reason   string
}

func NormalizeQuery(value string) (string, error) {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return "", ErrEmptyQuery
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", ErrEmptyQuery
	}
	return strings.Join(fields, " "), nil
}

func ValidateEvent(event SearchEvent) error {
	if event.EventID == "" || event.SessionID == "" || event.OccurredAt.IsZero() {
		return ErrInvalidEvent
	}
	_, err := NormalizeQuery(event.Query)
	if err != nil {
		return err
	}
	return nil
}
