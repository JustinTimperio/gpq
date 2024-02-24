package schema

import "time"

type Item[d any] struct {
	// User
	Priority       int64
	Data           d
	ShouldEscalate bool
	EscalationRate time.Duration
	CanTimeout     bool
	Timeout        time.Duration

	// Internal
	SubmittedAt   time.Time
	LastEscalated time.Time
	Index         int
}
