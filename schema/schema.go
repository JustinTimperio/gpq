package schema

import (
	"time"

	"github.com/dgraph-io/badger/v4"
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
	DiskMaxDelay          time.Duration
	DiskCachePath         string
	DiskCacheCompression  bool
	LazyDiskCacheEnabled  bool
	LazyDiskBatchSize     int
	DiskEncryptionEnabled bool
	DiskEncryptionKey     []byte
	Logger                badger.Logger
}

type EnQueueOptions struct {
	ShouldEscalate bool
	EscalationRate time.Duration
	CanTimeout     bool
	Timeout        time.Duration
}
