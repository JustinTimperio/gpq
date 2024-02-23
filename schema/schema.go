package schema

import "time"

type Item[d any] struct {
	Priority       int64
	Data           d
	EscalationRate time.Duration
	SubmittedAt    time.Time
	LastEscalated  time.Time
	Index          int
}
