package schema

import (
	"time"
)

type Item[d any] struct {
	// User
	Priority       int64
	Data           d
	DiskUUID       []byte
	ShouldEscalate bool
	EscalationRate time.Duration
	CanTimeout     bool
	Timeout        time.Duration

	// Internal
	SubmittedAt   time.Time
	LastEscalated time.Time
	Index         int
	BatchNumber   uint64
	WasRestored   bool
}

type LazyMessageQueueItem struct {
	ID               []byte
	Data             []byte
	TransactionBatch uint64
	WasRestored      bool
}

type GPQOptions struct {
	NumberOfBatches       int
	DiskCacheEnabled      bool
	DiskCachePath         string
	DiskCacheCompression  bool
	LazyDiskCacheEnabled  bool
	LazyDiskBatchSize     int
	DiskEncryptionEnabled bool
	DiskEncryptionKey     []byte
}
