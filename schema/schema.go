package schema

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Item is used to store items in the GPQ
// Internal use only
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

// LazyMessageQueueItem is used to store items in the lazy disk cache
// Internal use only
type LazyMessageQueueItem struct {
	ID               []byte
	Data             []byte
	TransactionBatch uint64
	WasRestored      bool
}

// GPQOptions is used to configure the GPQ
type GPQOptions struct {
	// Logger provides a custom logger interface
	Logger badger.Logger
	// Number of buckets to create
	NumberOfBuckets int

	// DiskCacheEnabled is used to enable or disable the disk cache
	DiskCacheEnabled bool
	// DiskMaxDelay is the maximum delay to wait before writing to disk
	DiskMaxDelay time.Duration
	// DiskCachePath is the local path to the disk cache
	DiskCachePath string
	// DiskCacheCompression is used to enable or disable zstd compression
	DiskCacheCompression bool

	// LazyDiskCacheEnabled is used to enable or disable the lazy disk cache
	LazyDiskCacheEnabled bool
	// LazyDiskBatchSize is the number of items to write to disk at once
	LazyDiskBatchSize int

	// DiskEncryptionEnabled is used to enable or disable disk encryption
	DiskEncryptionEnabled bool
	// DiskEncryptionKey is the key used to encrypt the disk cache
	DiskEncryptionKey []byte
}

// EnQueueOptions is used to configure the EnQueue method
type EnQueueOptions struct {
	ShouldEscalate bool
	EscalationRate time.Duration
	CanTimeout     bool
	Timeout        time.Duration
}
