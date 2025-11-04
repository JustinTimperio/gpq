package queues

import (
	"errors"
	"fmt"
	"sync"

	"github.com/JustinTimperio/gpq/disk"
	"github.com/JustinTimperio/gpq/queues/gheap"
	"github.com/JustinTimperio/gpq/schema"
	"github.com/JustinTimperio/gpq/timekeeper"

	"github.com/cornelk/hashmap"
	"github.com/tidwall/btree"
)

type CorePriorityQueue[T any] struct {
	buckets               *hashmap.Map[uint, *priorityQueue[T]]
	bpq                   *btree.Set[uint]
	mux                   *sync.RWMutex
	itemsInQueue          uint
	disk                  *disk.Disk[T]
	options               schema.GPQOptions
	lazy_disk_delete_chan chan schema.DeleteMessage
}

func NewCorePriorityQueue[T any](options schema.GPQOptions, diskCache *disk.Disk[T], deleteChan chan schema.DeleteMessage) CorePriorityQueue[T] {
	buckets := hashmap.New[uint, *priorityQueue[T]]()
	for i := uint(0); i < options.MaxPriority; i++ {
		pq := newPriorityQueue[T]()
		buckets.Set(uint(i), &pq)
	}
	var bpq btree.Set[uint]

	return CorePriorityQueue[T]{
		buckets:               buckets,
		mux:                   &sync.RWMutex{},
		itemsInQueue:          0,
		bpq:                   &bpq,
		disk:                  diskCache,
		options:               options,
		lazy_disk_delete_chan: deleteChan,
	}
}

func (cpq *CorePriorityQueue[T]) ItemsInQueue() uint {
	cpq.mux.RLock()
	defer cpq.mux.RUnlock()
	return cpq.itemsInQueue
}

func (cpq *CorePriorityQueue[T]) ActiveBuckets() uint {
	cpq.mux.RLock()
	defer cpq.mux.RUnlock()
	return uint(cpq.bpq.Len())
}

func (cpq *CorePriorityQueue[T]) Enqueue(data *schema.Item[T]) error {
	cpq.mux.Lock()
	defer cpq.mux.Unlock()

	bucket, ok := cpq.buckets.Get(data.Priority)
	if !ok {
		return fmt.Errorf("priority bucket %d not found in queue", data.Priority)
	}

	cpq.bpq.Insert(data.Priority)
	gheap.Enqueue[T](bucket, data)
	cpq.itemsInQueue++

	return nil
}

func (cpq *CorePriorityQueue[T]) EnqueueBatch(data []*schema.Item[T]) []error {
	cpq.mux.Lock()
	defer cpq.mux.Unlock()

	var errors []error

	for _, item := range data {
		bucket, ok := cpq.buckets.Get(item.Priority)
		if !ok {
			errors = append(errors, fmt.Errorf("Core Priority Queue Error: No bucket found with priority %d", item.Priority))
			continue
		}

		cpq.bpq.Insert(item.Priority)
		gheap.Enqueue[T](bucket, item)
		cpq.itemsInQueue++
	}

	return errors

}

func (cpq *CorePriorityQueue[T]) Dequeue() (*schema.Item[T], error) {
	cpq.mux.Lock()
	defer cpq.mux.Unlock()

	var item *schema.Item[T]
	for {
		priority, ok := cpq.bpq.Min()
		if !ok {
			return nil, errors.New("Core Priority Queue Error: No items found in the queue")
		}

		bucket, ok := cpq.buckets.Get(priority)
		if !ok {
			return nil, errors.New("Core Priority Queue Error: Priority not found")
		}

		var err error
		item, err = gheap.Dequeue[T](bucket)
		if err != nil {
			if bucket.Len() == 0 {
				cpq.bpq.Delete(priority)
			} else {
				return nil, err
			}
		} else {
			break
		}

	}

	cpq.itemsInQueue--

	return item, nil
}

func (cpq *CorePriorityQueue[T]) DequeueBatch(batchSize uint) ([]*schema.Item[T], []error) {
	cpq.mux.Lock()
	defer cpq.mux.Unlock()

	if cpq.bpq.Len() == 0 {
		return nil, []error{errors.New("queue is empty, no items to dequeue")}
	}

	batch := make([]*schema.Item[T], 0, batchSize)
	for i := 0; i < int(batchSize); i++ {
		priority, ok := cpq.bpq.Min()
		if !ok {
			break
		}

		bucket, ok := cpq.buckets.Get(priority)
		if !ok {
			// Internal corruption detected - rollback items already dequeued
			for _, item := range batch {
				cpq.bpq.Insert(item.Priority)
				if b, found := cpq.buckets.Get(item.Priority); found {
					gheap.Enqueue[T](b, item)
					cpq.itemsInQueue++
				}
			}
			return nil, []error{fmt.Errorf("Core Priority Queue Error: Internal corruption - priority %d in btree but not in hashmap (attempted to dequeue %d items before error)", priority, len(batch))}
		}

		item, err := gheap.Dequeue[T](bucket)
		if err != nil {
			// Only error that can return is an empty queue error
			break
		}

		cpq.itemsInQueue--
		batch = append(batch, item)
		if bucket.Len() == 0 {
			cpq.bpq.Delete(priority)
		}
	}

	return batch, nil
}

func (cpq *CorePriorityQueue[T]) Prioritize() (removed uint, escalated uint, err error) {
	// Collect bucket keys with read lock
	var bucketKeys []uint
	cpq.mux.RLock()
	cpq.buckets.Range(func(key uint, bucket *priorityQueue[T]) bool {
		bucketKeys = append(bucketKeys, key)
		return true
	})
	cpq.mux.RUnlock()

	// Process each bucket with individual locks
	for _, key := range bucketKeys {
		cpq.mux.Lock()

		bucket, ok := cpq.buckets.Get(key)
		if !ok {
			cpq.mux.Unlock()
			continue // Bucket was removed, skip
		}

		// Process timeouts in this bucket
		var len = bucket.Len()
		var currentIndex uint
		for i := 0; i < len; i++ {
			item := bucket.items[currentIndex]

			if item.CanTimeout {
				currentTime := timekeeper.Now()
				if currentTime.Sub(item.SubmittedAt) > item.Timeout {

					if cpq.options.DiskCacheEnabled {
						if cpq.options.LazyDiskCacheEnabled {
							dm := schema.DeleteMessage{
								BatchNumber: item.BatchNumber,
								DiskUUID:    item.DiskUUID,
								WasRestored: item.WasRestored,
							}

							cpq.lazy_disk_delete_chan <- dm
						} else {
							cpq.disk.DeleteSingle(item.DiskUUID)
						}
					}

					_, e := gheap.Remove[T](bucket, item)
					if e != nil {
						err = fmt.Errorf("Core Priority Queue Error: %w", err)
						cpq.mux.Unlock()
						return removed, escalated, err
					}
					cpq.itemsInQueue--
					removed++

				} else {
					currentIndex++
				}

			} else {
				currentIndex++
			}
		}

		cpq.mux.Unlock()
	}

	// Clean up empty buckets
	cpq.mux.Lock()
	for _, key := range bucketKeys {
		bucket, ok := cpq.buckets.Get(key)
		if ok && bucket.Len() == 0 {
			cpq.bpq.Delete(key)
		}
	}
	cpq.mux.Unlock()

	// Process escalations with per-bucket locking
	for _, key := range bucketKeys {
		cpq.mux.Lock()

		bucket, ok := cpq.buckets.Get(key)
		if !ok {
			cpq.mux.Unlock()
			continue
		}

		var lastItemWasEscalated bool
		var len = bucket.Len()

		for i := 0; i < len; i++ {
			item := bucket.items[i]

			// Reset flag at start of each iteration
			wasEscalatedThisRound := false

			if item.ShouldEscalate {
				currentTime := timekeeper.Now()
				readyToEscalate := currentTime.Sub(item.LastEscalated) > item.EscalationRate
				canMove := !lastItemWasEscalated && i != 0

				if readyToEscalate && canMove {
					item.LastEscalated = currentTime
					bucket.UpdatePriority(item, i-1)
					escalated++
					wasEscalatedThisRound = true
				}
			}

			lastItemWasEscalated = wasEscalatedThisRound
		}

		cpq.mux.Unlock()
	}

	return removed, escalated, nil
}
