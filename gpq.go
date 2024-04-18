package gpq

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq/schema"

	"github.com/cornelk/hashmap"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/google/uuid"
)

// GPQ is a generic priority queue that supports priority levels and timeouts
// It is implemented using a heap for each priority level and a priority queue of non-empty buckets
// It also supports disk caching using badgerDB with the option to lazily disk writes and deletes
// The GPQ is thread-safe and supports concurrent access
type GPQ[d any] struct {
	// BucketCount is the number of priority buckets
	BucketCount int64
	// NonEmptyBuckets is a priority queue of non-empty buckets
	NonEmptyBuckets *BucketPriorityQueue

	// buckets is a map of priority buckets
	buckets *hashmap.Map[int64, *CorePriorityQueue[d]]
	// bucketPrioritizeLockMap is a map of locks for each priority bucket
	bucketPrioritizeLockMap *hashmap.Map[int64, *sync.Mutex]
	// activeDBSessions is a wait group for active disk cache sessions
	activeDBSessions *sync.WaitGroup
	// diskCache is a badgerDB used to store items in the GPQ
	diskCache *badger.DB
	// diskCacheEnabled is a boolean that indicates if the disk cache is enabled
	diskCacheEnabled bool
	// lazyDiskCache is a boolean that indicates if the disk cache should be lazily deleted
	lazyDiskCache bool
	// lazyDiskMessageChan is a channel used to send messages to the lazy disk cache
	lazyDiskSendChan chan schema.LazyMessageQueueItem
	// lazyDiskDeleteChan is a channel used to send messages to the lazy disk cache
	lazyDiskDeleteChan chan schema.LazyMessageQueueItem
	// allBatchesSynced is a channel used to signal when all batches have been synced
	allBatchesSynced *sync.WaitGroup
	// syncedBatches is a map of all synced batches
	syncedBatches *sync.Map
	// batchNumber is the current batch number
	batchNumber uint64
	// batchCounter is the counter for the current batch
	batchCounter uint64
	// batchSize is the size of each batch that is sent to the disk cache or deleted
	batchSize int64
}

// NewGPQ creates a new GPQ with the given number of buckets
// The number of buckets is the number of priority levels you want to support
// You must provide the number of buckets ahead of time and all priorities you submit
// must be within the range of 0 to NumOfBuckets
func NewGPQ[d any](Options schema.GPQOptions) (uint64, *GPQ[d], error) {
	bp := NewBucketPriorityQueue()

	gpq := &GPQ[d]{
		BucketCount:     int64(Options.NumberOfBatches),
		NonEmptyBuckets: bp,

		buckets:                 hashmap.New[int64, *CorePriorityQueue[d]](),
		diskCacheEnabled:        Options.DiskCacheEnabled,
		activeDBSessions:        &sync.WaitGroup{},
		bucketPrioritizeLockMap: hashmap.New[int64, *sync.Mutex](),
		lazyDiskCache:           Options.DiskCacheEnabled,
		lazyDiskSendChan:        make(chan schema.LazyMessageQueueItem),
		lazyDiskDeleteChan:      make(chan schema.LazyMessageQueueItem),
		allBatchesSynced:        &sync.WaitGroup{},
		syncedBatches:           &sync.Map{},
		batchNumber:             1,
		batchSize:               int64(Options.LazyDiskBatchSize),
	}

	for i := 0; i < Options.NumberOfBatches; i++ {
		pq := NewCorePriorityQueue[d](bp)
		gpq.buckets.Set(int64(i), &pq)
		gpq.bucketPrioritizeLockMap.Set(int64(i), &sync.Mutex{})
	}

	var reEnqueued uint64
	if Options.DiskCacheEnabled {

		// Open the disk cache and restore items to the GPQ
		if Options.DiskCachePath == "" {
			return reEnqueued, gpq, errors.New("Disk cache path is required")
		}

		opts := badger.DefaultOptions(Options.DiskCachePath)
		opts.Logger = Options.Logger

		if Options.DiskCacheCompression {
			opts.Compression = options.ZSTD
		}
		if Options.DiskEncryptionEnabled {
			opts.WithEncryptionKey(Options.DiskEncryptionKey)
		}
		db, err := badger.Open(opts)
		if err != nil {
			return reEnqueued, nil, errors.New("Error opening disk cache: " + err.Error())
		}
		gpq.diskCache = db

		// Start the lazy disk loader and deleter
		// This allows for items to be committed to the queue
		// without waiting for a full disk sync of messages
		if Options.LazyDiskCacheEnabled {
			if Options.DiskMaxDelay == 0 {
				// Default the batch send delay to 1/4 of a second
				Options.DiskMaxDelay = 250 * time.Millisecond
			}
			go gpq.lazyDiskLoader(Options.DiskMaxDelay)
			go gpq.lazyDiskDeleter()
		}

		// Re-add items to the GPQ from the disk cache
		err = gpq.diskCache.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				var value []byte
				item := it.Item()
				key := item.Key()

				// Get the item from the disk cache
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				item.Value(func(val []byte) error {
					value = append([]byte{}, val...)
					return nil
				})

				if len(value) == 0 {
					return errors.New("Error reading from disk cache: value is empty")
				}

				// Decode the item
				var buf bytes.Buffer
				buf.Write(value)
				obj := schema.Item[d]{}
				err = gob.NewDecoder(&buf).Decode(&obj)
				if err != nil {
					return errors.New("Error decoding item from disk cache: " + err.Error())
				}

				// Re-enqueue the item with the same parameters it had when it was enqueued
				err = gpq.reQueue(obj.Data, obj.Priority, obj.ShouldEscalate, obj.EscalationRate, obj.CanTimeout, obj.Timeout, obj.DiskUUID)
				if err != nil {
					return err
				}

				reEnqueued++
			}

			return nil
		})
		if err != nil {
			return reEnqueued, nil, errors.New("Error reading from disk cache: " + err.Error())
		}

	} else {
		gpq.diskCache = nil
	}

	return reEnqueued, gpq, nil
}

// EnQueue adds an item to the GPQ
// The priorityBucket is the priority level of the item
// The escalationRate is the amount of time before the item is escalated to the next priority level
// The data is the data you want to store in the GPQ item
func (g *GPQ[d]) EnQueue(data d, priorityBucket int64, options schema.EnQueueOptions) error {

	if priorityBucket > g.BucketCount {
		return errors.New("Priority bucket does not exist")
	}

	// Create the item
	obj := schema.Item[d]{
		Data:           data,
		Priority:       priorityBucket,
		ShouldEscalate: options.CanTimeout,
		EscalationRate: options.EscalationRate,
		CanTimeout:     options.CanTimeout,
		Timeout:        options.Timeout,
		SubmittedAt:    time.Now(),
		LastEscalated:  time.Now(),
	}

	pq, _ := g.buckets.Get(priorityBucket)

	if g.diskCacheEnabled {

		// Generate a UUID for the item
		key, err := uuid.New().MarshalBinary()
		if err != nil {
			return err
		}

		obj.DiskUUID = key

		// Set the batch number for the item
		var bnum uint64
		if atomic.AddUint64(&g.batchCounter, 1)%uint64(g.batchSize) == 0 {
			bnum = atomic.AddUint64(&g.batchNumber, 1)
		} else {
			bnum = atomic.LoadUint64(&g.batchNumber)
		}
		g.syncedBatches.Store(g.batchNumber, false)

		// Encode the item
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(obj)
		if err != nil {
			return err
		}
		value := buf.Bytes()

		// Send the item to the disk cache
		if g.lazyDiskCache {
			g.lazyDiskSendChan <- schema.LazyMessageQueueItem{
				ID:               key,
				Data:             value,
				TransactionBatch: bnum,
			}

		} else {
			err = g.diskCache.Update(func(txn *badger.Txn) error {
				return txn.Set(key, value)
			})
			if err != nil {
				return err
			}
		}
	}

	pq.EnQueue(obj)

	return nil
}

// reQueue adds an item to the GPQ with a specific key restored from the disk cache
func (g *GPQ[d]) reQueue(data d, priorityBucket int64, escalate bool, escalationRate time.Duration, canTimeout bool, timeout time.Duration, key []byte) error {

	if priorityBucket > g.BucketCount {
		return errors.New("Priority bucket does not exist")
	}

	// Create the item
	obj := schema.Item[d]{
		Data:           data,
		Priority:       priorityBucket,
		ShouldEscalate: escalate,
		EscalationRate: escalationRate,
		CanTimeout:     canTimeout,
		Timeout:        timeout,
		SubmittedAt:    time.Now(),
		LastEscalated:  time.Now(),
		DiskUUID:       key,
		WasRestored:    true,
	}

	pq, _ := g.buckets.Get(priorityBucket)
	pq.EnQueue(obj)

	return nil
}

// DeQueue removes and returns the item with the highest priority from the GPQ.
// It returns the priority of the item, the data associated with it,
// and an error if the queue is empty or if any internal data structures are missing.
func (g *GPQ[d]) DeQueue() (priority int64, data d, err error) {

	// Return an error if there are no items in the queue
	if atomic.LoadUint64(&g.NonEmptyBuckets.ObjectsInQueue) == 0 && atomic.LoadInt64(&g.NonEmptyBuckets.ActiveBuckets) == 0 {
		return -1, data, errors.New("No items in any queue")
	}

	// Get the first item from the highest priority bucket
	// If the bucket is empty, remove it from the non-empty buckets
	// This structure allows for O(1) access to the highest priority item
	priorityBucket, exists := g.NonEmptyBuckets.Peek()
	if !exists {
		return -1, data, errors.New("No item in queue bucket")
	}

	pq, _ := g.buckets.Get(priorityBucket)

	// Dequeue the item
	wasRestored, batchNumber, key, p, item, err := pq.DeQueue()
	if err != nil {
		return -1, data, err
	}
	priority = p

	if !g.diskCacheEnabled {
		data = item

	} else {
		data = item
		if g.lazyDiskCache {
			g.lazyDiskDeleteChan <- schema.LazyMessageQueueItem{
				ID:               key,
				Data:             nil,
				TransactionBatch: batchNumber,
				WasRestored:      wasRestored,
			}
		} else {
			// Get the item from the disk cache
			var value []byte
			err = g.diskCache.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					value = append([]byte{}, val...)
					return nil
				})
			})
			if err != nil {
				return -1, data, err
			}

			var buf bytes.Buffer
			buf.Write(value)
			obj := schema.Item[d]{}
			err = gob.NewDecoder(&buf).Decode(&obj)
			if err != nil {
				return -1, data, err
			}
			data = obj.Data

			// Lazily delete the item from the disk cache
			g.activeDBSessions.Add(1)
			go func() {
				defer g.activeDBSessions.Done()

				// Delete the item from the disk cache
				g.diskCache.Update(func(txn *badger.Txn) error {
					return txn.Delete(key)
				})

			}()
		}
	}

	return priority, data, nil

}

// Prioritize is a method of the GPQ type that prioritizes items within a heap.
// It iterates over each bucket in the GPQ, locks the corresponding mutex, and checks if there are items to prioritize.
// If there are items, it calculates the number of durations that have passed since the last escalation and updates the priority accordingly.
// The method uses goroutines to process each bucket concurrently, improving performance.
// It returns an error if any of the required data structures are missing or if there are no items to prioritize.
func (g *GPQ[d]) Prioritize() (timedOutItems uint64, escalatedItems uint64, errs []error) {

	for bucketID := 0; bucketID < int(g.BucketCount); bucketID++ {

		pq, _ := g.buckets.Get(int64(bucketID))
		mutex, _ := g.bucketPrioritizeLockMap.Get(int64(bucketID))
		mutex.Lock()
		pointers := pq.ReadPointers()

		if len(pointers) == 0 {
			errs = append(errs, errors.New("No items to prioritize in heap: "+fmt.Sprintf("%d", bucketID)))
			mutex.Unlock()
			continue
		}

		evalTime := time.Now()
		for _, pointer := range pointers {

			if pointer == nil {
				continue
			}

			// Remove the item if it has timed out
			if pointer.CanTimeout {
				duration := int(math.Abs(float64(pointer.SubmittedAt.Sub(evalTime).Milliseconds())))
				if duration > int(pointer.Timeout.Milliseconds()) {

					// Remove the item from the priority queue
					pq.Remove(pointer)
					atomic.AddUint64(&timedOutItems, 1)

					// Remove the item from the disk cache
					if g.diskCacheEnabled {
						g.activeDBSessions.Add(1)
						go func() {
							defer g.activeDBSessions.Done()
							g.diskCache.Update(func(txn *badger.Txn) error {
								return txn.Delete(pointer.DiskUUID)
							})
						}()

						continue
					}
				}
			}

			// Escalate the priority if the item hasn't timed out and can escalate
			if pointer.ShouldEscalate {
				// Calculate the number of durations that fit between evalTime and pointer.SubmittedAt
				duration := int(math.Abs(float64(pointer.LastEscalated.Sub(evalTime).Milliseconds())))
				numDurations := duration / int(pointer.EscalationRate.Milliseconds())

				// If the number of durations is greater than 0, escalate the priority
				if numDurations > 0 {
					pointer.Priority = pointer.Priority - int64(numDurations)
					if pointer.Priority < 0 {
						pointer.Priority = 0
					}
					pointer.LastEscalated = evalTime
					pq.UpdatePriority(pointer, pointer.Priority)
					atomic.AddUint64(&escalatedItems, 1)
				}
			}
		}
		mutex.Unlock()
	}

	return timedOutItems, escalatedItems, errs
}

// Peek returns the item with the highest priority from the GPQ.
// It returns the priority of the item, the data associated with it,
// and an error if the queue is empty or if any internal data structures are missing.
func (g *GPQ[d]) Peek() (data d, err error) {

	// Return an error if there are no items in the queue
	if atomic.LoadUint64(&g.NonEmptyBuckets.ObjectsInQueue) == 0 && atomic.LoadInt64(&g.NonEmptyBuckets.ActiveBuckets) == 0 {
		return data, errors.New("No items in any queue")
	}

	// Get the first item from the highest priority bucket
	// If the bucket is empty, remove it from the non-empty buckets
	// This structure allows for O(1) access to the highest priority item
	priorityBucket, exists := g.NonEmptyBuckets.Peek()
	if !exists {
		return data, errors.New("No item in queue bucket")
	}

	pq, _ := g.buckets.Get(priorityBucket)

	// Dequeue the item
	item, err := pq.Peek()
	if err != nil {
		return item, err
	}

	return item, nil

}

func (g *GPQ[d]) lazyDiskLoader(maxDelay time.Duration) {
	g.activeDBSessions.Add(1)
	defer g.activeDBSessions.Done()

	g.allBatchesSynced.Add(1)
	defer g.allBatchesSynced.Done()

	batch := make(map[uint64][]schema.LazyMessageQueueItem, 0)
	ticker := time.NewTicker(maxDelay)

	for {
		select {
		case item, ok := <-g.lazyDiskSendChan:
			if !ok {
				// Channel is closed, process remaining batch and return
				for k, v := range batch {
					g.processBatch(v)
					batch[k] = batch[k][:0]
					g.syncedBatches.Store(k, true)
				}
				return
			}

			batch[item.TransactionBatch] = append(batch[item.TransactionBatch], item)

			if len(batch[item.TransactionBatch]) >= int(g.batchSize) {
				synced, ok := g.syncedBatches.Load(item.TransactionBatch)
				if ok && synced.(bool) == true {
					g.processBatch(batch[item.TransactionBatch])
					batch[item.TransactionBatch] = batch[item.TransactionBatch][:0]
					g.syncedBatches.Store(item.TransactionBatch, true)
				}
			}

		case <-ticker.C:
			for k, v := range batch {
				if len(v) >= int(g.batchSize) {
					g.processBatch(v)
					batch[k] = batch[k][:0]
					g.syncedBatches.Store(k, true)
				}
			}
		}
	}
}

func (g *GPQ[d]) lazyDiskDeleter() {
	g.activeDBSessions.Add(1)
	defer g.activeDBSessions.Done()

	batch := make(map[uint64][]schema.LazyMessageQueueItem, 0)
	restored := make([]schema.LazyMessageQueueItem, 0)

	for {
		select {
		case item, ok := <-g.lazyDiskDeleteChan:
			if !ok {

				g.allBatchesSynced.Wait()

				g.deleteBatch(restored)
				// Channel is closed, process remaining batch and return
				for _, v := range batch {
					g.deleteBatch(v)
					batch[item.TransactionBatch] = batch[item.TransactionBatch][:0]
					g.syncedBatches.Delete(item.TransactionBatch)
				}
				return
			}

			if item.WasRestored {
				restored = append(restored, item)
				if len(restored) >= int(g.batchSize) {
					g.deleteBatch(restored)
					restored = restored[:0]
				}

			} else {
				// Add the item to the batch
				batch[item.TransactionBatch] = append(batch[item.TransactionBatch], item)

				// If the batch is full, process it and delete the items from the disk cache
				if len(batch[item.TransactionBatch]) >= int(g.batchSize) {
					synced, ok := g.syncedBatches.Load(item.TransactionBatch)
					if ok && synced.(bool) == true {
						g.deleteBatch(batch[item.TransactionBatch])
						batch[item.TransactionBatch] = batch[item.TransactionBatch][:0]
						g.syncedBatches.Delete(item.TransactionBatch)
					}
				}
			}
		}
	}
}

func (g *GPQ[d]) processBatch(batch []schema.LazyMessageQueueItem) error {
	txn := g.diskCache.NewTransaction(true) // Read-write transaction
	defer txn.Discard()

	for i := 0; i < len(batch); i++ {
		entry := batch[i]
		err := txn.Set(entry.ID, entry.Data)
		if err == badger.ErrTxnTooBig {
			// Commit the transaction and start a new one
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = g.diskCache.NewTransaction(true)
			txn.Set(entry.ID, entry.Data)
		} else if err != nil {
			return err
		}
	}

	// Commit the final transaction, if it has any pending writes
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (g *GPQ[d]) deleteBatch(batch []schema.LazyMessageQueueItem) error {
	txn := g.diskCache.NewTransaction(true) // Read-write transaction
	defer txn.Discard()

	for i := 0; i < len(batch); i++ {
		entry := batch[i]
		err := txn.Delete(entry.ID)
		if err == badger.ErrTxnTooBig {
			// Commit the transaction and start a new one
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = g.diskCache.NewTransaction(true)
			txn.Delete(entry.ID)
		} else if err != nil {
			return err
		}
	}

	// Commit the final transaction, if it has any pending writes
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (g *GPQ[d]) Close() {
	close(g.lazyDiskSendChan)
	close(g.lazyDiskDeleteChan)

	// Wait for all db sessions to sync to disk
	g.activeDBSessions.Wait()

	// Sync the disk cache
	if g.diskCacheEnabled {
		g.diskCache.Sync()
		g.diskCache.Close()
	}
}
