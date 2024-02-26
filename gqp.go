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
	"github.com/google/uuid"
)

// GPQ is a priority queue that supports priority escalation
type GPQ[d any] struct {
	// TotalLen is the total length of the GPQ
	TotalLen uint64
	// Buckets is a map of priority buckets
	Buckets *hashmap.Map[int64, *CorePriorityQueue[d]]
	// MutexMap is a map of mutexes for each priority bucket
	// This is used to lock the priority bucket when enqueuing and dequeuing
	BucketMutexMap *hashmap.Map[int64, *sync.Mutex]
	// LenMap is a map of the length of each priority bucket
	BucketsLenMap *hashmap.Map[int64, uint64]
	// NonEmptyBuckets is a priority queue of non-empty buckets
	NonEmptyBuckets *BucketPriorityQueue[d]
	// NonEmptyBucketsMutex is a mutex for the non-empty buckets
	NonEmptyBucketsMutex *sync.Mutex
	// ActiveDBSessions is a wait group for active disk cache sessions
	// It allows for items to be removed from the disk cache without waiting for a full disk sync
	ActiveDBSessions *sync.WaitGroup
	// DiskCache is a badgerDB used to store items in the GPQ
	DiskCache *badger.DB
	// IsDiskCacheEnabled is a boolean that indicates if the disk cache is enabled
	DiskCacheEnabled bool
}

// NewGPQ creates a new GPQ with the given number of buckets
// The number of buckets is the number of priority levels you want to support
// You must provide the number of buckets ahead of time and all priorities you submit
// must be within the range of 0 to NumOfBuckets
func NewGPQ[d any](NumOfBuckets int, diskCache bool, diskCachePath string) (*GPQ[d], error) {
	bp := NewBucketPriorityQueue[d]()

	gpq := &GPQ[d]{
		Buckets:              hashmap.New[int64, *CorePriorityQueue[d]](),
		BucketsLenMap:        hashmap.New[int64, uint64](),
		BucketMutexMap:       hashmap.New[int64, *sync.Mutex](),
		NonEmptyBuckets:      bp,
		DiskCacheEnabled:     diskCache,
		NonEmptyBucketsMutex: &sync.Mutex{},
		ActiveDBSessions:     &sync.WaitGroup{},
	}

	for i := 0; i < NumOfBuckets; i++ {
		pq := NewCorePriorityQueue[d]()
		gpq.Buckets.Set(int64(i), &pq)
		gpq.BucketsLenMap.Set(int64(i), 0)
		gpq.BucketMutexMap.Set(int64(i), &sync.Mutex{})
	}

	if diskCache {
		if diskCachePath == "" {
			return gpq, errors.New("Disk cache path is required")
		}
		opts := badger.DefaultOptions(diskCachePath)
		opts.Logger = nil
		db, err := badger.Open(opts)
		if err != nil {
			return nil, err
		}
		gpq.DiskCache = db
		var reEnqueued uint64

		// Re-add items to the GPQ from the disk cache
		err = gpq.DiskCache.View(func(txn *badger.Txn) error {
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

				// Get the item stored in the disk cache
				item.Value(func(val []byte) error {
					value = append([]byte{}, val...)
					return nil
				})

				// Decode the item
				var buf bytes.Buffer
				buf.Write(value)
				obj := schema.Item[d]{}
				err = gob.NewDecoder(&buf).Decode(&obj)
				if err != nil {
					return err
				}

				// Re-enqueue the item with the same parameters it had when it was enqueued
				// This whole process is slow since we need remove the item from the disk cache
				// And then re-enqueue it into the GPQ which is itself another disk cache write operation
				err = gpq.EnQueue(obj.Data, obj.Priority, obj.ShouldEscalate, obj.EscalationRate, obj.CanTimeout, obj.Timeout)
				if err != nil {
					return err
				}

				// Delete the item from the disk cache
				err = gpq.DiskCache.Update(func(txn *badger.Txn) error {
					return txn.Delete(key)
				})
				if err != nil {
					return err
				}

				reEnqueued++
			}

			return nil
		})
		if err != nil {
			return nil, err
		}

	} else {
		gpq.DiskCache = nil
	}

	return gpq, nil
}

// EnQueue adds an item to the GPQ
// The priorityBucket is the priority level of the item
// The escalationRate is the amount of time before the item is escalated to the next priority level
// The data is the data you want to store in the GPQ item
func (g *GPQ[d]) EnQueue(data d, priorityBucket int64, escalate bool, escalationRate time.Duration, canTimeout bool, timeout time.Duration) error {

	if priorityBucket > int64(g.Buckets.Len()) {
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
	}

	mutex, _ := g.BucketMutexMap.Get(priorityBucket)
	count, _ := g.BucketsLenMap.Get(priorityBucket)
	pq, _ := g.Buckets.Get(priorityBucket)

	if !g.DiskCacheEnabled {
		// Enqueue the item directly into the priority bucket
		mutex.Lock()
		pq.EnQueue(obj)
		mutex.Unlock()
	} else {

		// Generate a UUID for the item
		key, err := uuid.New().MarshalBinary()
		if err != nil {
			return err
		}

		obj.DiskUUID = key

		// Encode the item
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(obj)
		if err != nil {
			return err
		}
		value := buf.Bytes()

		// Send the item to the disk cache
		err = g.DiskCache.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		if err != nil {
			return err
		}

		// Enqueue the items UUID
		mutex.Lock()
		pq.EnQueue(obj)
		mutex.Unlock()
	}

	if !g.NonEmptyBuckets.Contains(priorityBucket) {
		g.NonEmptyBucketsMutex.Lock()
		g.NonEmptyBuckets.Add(priorityBucket)
		g.NonEmptyBucketsMutex.Unlock()
	}

	// Increment the length of the priority bucket and the total length
	atomic.AddUint64(&count, 1)
	atomic.AddUint64(&g.TotalLen, 1)

	return nil
}

// DeQueue removes and returns the item with the highest priority from the GPQ.
// It returns the priority of the item, the data associated with it,
// and an error if the queue is empty or if any internal data structures are missing.
func (g *GPQ[d]) DeQueue() (priority int64, data d, err error) {

	// Get the first item from the highest priority bucket
	// If the bucket is empty, remove it from the non-empty buckets
	// This structure allows for O(1) access to the highest priority item
	// Although its actually O(n) * (cache miss + cache hits)
	nonEmpty := g.NonEmptyBuckets.Len()

	for nonEmpty > 0 {
		nonEmpty--
		// Get the first non-empty bucket
		priorityBucket, exists := g.NonEmptyBuckets.Peek()
		if !exists {
			// Keep trying to get the first non-empty bucket
			continue
		}

		mutex, _ := g.BucketMutexMap.Get(priorityBucket)
		count, _ := g.BucketsLenMap.Get(priorityBucket)
		pq, _ := g.Buckets.Get(priorityBucket)

		if !g.DiskCacheEnabled {
			// Dequeue the item
			mutex.Lock()
			_, p, item, err := pq.DeQueue()
			mutex.Unlock()
			data = item
			priority = p

			if err != nil {
				g.NonEmptyBucketsMutex.Lock()
				g.NonEmptyBuckets.Remove(priorityBucket)
				g.NonEmptyBucketsMutex.Unlock()
				continue
			}

		} else {
			// Get the item from the disk cache
			mutex.Lock()
			key, p, _, err := pq.DeQueue()
			mutex.Unlock()

			if err != nil {
				g.NonEmptyBucketsMutex.Lock()
				g.NonEmptyBuckets.Remove(priorityBucket)
				g.NonEmptyBucketsMutex.Unlock()
				continue
			}

			priority = p

			// Get the item from the disk cache
			var value []byte
			err = g.DiskCache.View(func(txn *badger.Txn) error {
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
			g.ActiveDBSessions.Add(1)
			go func() {
				defer g.ActiveDBSessions.Done()

				// Delete the item from the disk cache
				g.DiskCache.Update(func(txn *badger.Txn) error {
					return txn.Delete(key)
				})

			}()
		}

		// Decrement the length of the priority bucket and the total length
		// ^uint64(0) is the same as -1 (idk some cursed ass fuckery)
		atomic.AddUint64(&count, ^uint64(0))
		atomic.AddUint64(&g.TotalLen, ^uint64(0))
		return priority, data, nil
	}

	return -1, data, errors.New("No items in ANY queue")

}

// Prioritize is a method of the GPQ type that prioritizes items within a heap.
// It iterates over each bucket in the GPQ, locks the corresponding mutex, and checks if there are items to prioritize.
// If there are items, it calculates the number of durations that have passed since the last escalation and updates the priority accordingly.
// The method uses goroutines to process each bucket concurrently, improving performance.
// It returns an error if any of the required data structures are missing or if there are no items to prioritize.
func (g *GPQ[d]) Prioritize() (timedOutItems uint64, escalatedItems uint64, errs []error) {
	wg := sync.WaitGroup{}
	errCh := make(chan error, g.Buckets.Len()) // Channel to collect errors

	for i := 0; i < int(g.Buckets.Len()); i++ {
		wg.Add(1)

		go func(bucketID int64) {
			defer wg.Done()
			mutex, _ := g.BucketMutexMap.Get(bucketID)
			pq, _ := g.Buckets.Get(bucketID)

			_, err := pq.Peek()
			if err != nil {
				errCh <- errors.New("No items to prioritize in heap: " + fmt.Sprintf("%d", bucketID))
				return
			}

			mutex.Lock()
			defer mutex.Unlock()

			pointers := pq.ReadPointers()
			evalTime := time.Now()
			for _, pointer := range pointers {

				// Remove the item if it has timed out
				if pointer.CanTimeout {
					duration := int(math.Abs(float64(pointer.SubmittedAt.Sub(evalTime).Milliseconds())))
					if duration > int(pointer.Timeout.Milliseconds()) {

						// Remove the item from the priority queue
						pq.Remove(pointer)
						atomic.AddUint64(&timedOutItems, 1)

						// Remove the item from the disk cache
						if g.DiskCacheEnabled {
							g.ActiveDBSessions.Add(1)
							go func() {
								defer g.ActiveDBSessions.Done()
								g.DiskCache.Update(func(txn *badger.Txn) error {
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
		}(int64(i))
	}

	go func() {
		wg.Wait()
		close(errCh) // Close the error channel after all goroutines have completed
	}()

	// Collect errors from the error channel
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return timedOutItems, escalatedItems, errs
}
