package schema

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Item is used to store items in the GPQ
type Item[d any] struct {
	// User
	Priority       uint
	Data           d
	DiskUUID       []byte
	ShouldEscalate bool
	EscalationRate time.Duration
	CanTimeout     bool
	Timeout        time.Duration

	// Internal
	SubmittedAt      time.Time
	LastEscalated    time.Time
	Index            int
	InternalPriority int
	BatchNumber      uint
	WasRestored      bool
}

// NewItem creates a new item using the options provide and data stored in a generic
func NewItem[d any](priority uint, data d, options EnqueueOptions) Item[d] {
	return Item[d]{
		Priority:       priority,
		Data:           data,
		ShouldEscalate: options.ShouldEscalate,
		EscalationRate: options.EscalationRate,
		CanTimeout:     options.CanTimeout,
		Timeout:        options.Timeout,
		SubmittedAt:    time.Now(),
	}
}

// ToBytes converts a item into a blob
func (i *Item[d]) ToBytes() ([]byte, error) {
	// Encode the item to a byte slice
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(i)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// FromBytes converts a blob to a item
func (i *Item[d]) FromBytes(data []byte) error {
	// Decode the item from a byte slice
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(i)
	if err != nil {
		return err
	}

	return nil
}

// GPQOptions is used to configure the GPQ
type GPQOptions struct {
	// Logger provides a custom logger interface for the Badger cache
	Logger badger.Logger
	// MaxPriority is the maximum priority allowed by this GPQ
	MaxPriority uint

	// DiskCacheEnabled is used to enable or disable the disk cache
	DiskCacheEnabled bool
	// DiskCachePath is the local path to the disk cache directory
	DiskCachePath string
	// DiskWriteDelay is the delay between writes to disk (used to batch writes)
	DiskWriteDelay time.Duration
	// DiskCacheCompression is used to enable or disable zstd compression on the disk cache
	DiskCacheCompression bool

	// LazyDiskCacheEnabled is used to enable or disable the lazy disk cache
	LazyDiskCacheEnabled bool
	// LazyDiskBatchSize is the number of items to write to disk at once
	LazyDiskBatchSize uint
	// LazyDiskCacheChannelSize is the length of the channel buffer for the disk cache
	LazyDiskCacheChannelSize uint

	// DiskEncryptionEnabled is used to enable or disable disk encryption
	DiskEncryptionEnabled bool
	// DiskEncryptionKey is the key used to encrypt the disk cache
	DiskEncryptionKey []byte
}

// EnqueueOptions is used to configure the EnQueue method
type EnqueueOptions struct {
	// ShouldEscalate is used to determine if the item should be escalated
	ShouldEscalate bool
	// EscalationRate is the time to wait before escalating the item (happens every duration)
	EscalationRate time.Duration
	// CanTimeout is used to determine if the item can timeout
	CanTimeout bool
	// Timeout is the time to wait before timing out the item
	Timeout time.Duration
}

// Used internally to delete entries on the lazy disk chan
type DeleteMessage struct {
	BatchNumber uint
	DiskUUID    []byte
	WasRestored bool
}
