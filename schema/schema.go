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
}

type LazyMessageQueueItem struct {
	ID               []byte
	Data             []byte
	TransactionBatch uint64
}

type GPQOptions struct {
	NumberOfBuckets   int
	DiskCache         bool
	DiskCachePath     string
	Compression       bool
	LazyDiskBatchSize int

	RaftPoolConnections int
	RaftStableStorePath string
	RaftSnapshotPath    string
	RaftSnapshotRetain  int
	RaftCacheSize       int
	RaftPort            int
	RaftBindAddress     string
	RaftNodeID          string
}
