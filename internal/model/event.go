package model

import (
	"fmt"
	"strings"
	"time"
)

type SearchEvent struct {
	EventID    string    `json:"event_id"`
	Query      string    `json:"query"`
	SessionID  string    `json:"session_id"`
	UserID     string    `json:"user_id,omitempty"`
	Source     string    `json:"source,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

type TopItem struct {
	Query string `json:"query"`
	Count int    `json:"count"`
}

type TopResponse struct {
	WindowSeconds int       `json:"window_seconds"`
	Limit         int       `json:"limit"`
	Items         []TopItem `json:"items"`
}

type StopWord struct {
	Word string `json:"word"`
}

func NormalizeQuery(value string) (string, error) {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return "", fmt.Errorf("query is empty")
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", fmt.Errorf("query is empty")
	}
	return strings.Join(fields, " "), nil
}
