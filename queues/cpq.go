package queues

import (
	"errors"
	"fmt"
	"sync"

	"github.com/JustinTimperio/gpq/disk"
	"github.com/JustinTimperio/gpq/ftime"
	"github.com/JustinTimperio/gpq/queues/gheap"
	"github.com/JustinTimperio/gpq/schema"

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
		return errors.New("Core Priority Queue Error: Priority not found")
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
		return nil, []error{errors.New("Core Priority Queue Error: No items found in the queue")}
	}

	batch := make([]*schema.Item[T], 0, batchSize)
	for i := 0; i < int(batchSize); i++ {
		priority, ok := cpq.bpq.Min()
		if !ok {
			break
		}

		bucket, ok := cpq.buckets.Get(priority)
		if !ok {
			return batch, []error{errors.New("Core Priority Queue Error: Internal error, priority not found")}
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
	cpq.mux.Lock()
	defer cpq.mux.Unlock()

	cpq.buckets.Range(func(key uint, bucket *priorityQueue[T]) bool {
		// Iterate through the bucket and remove items that have been waiting too long
		var len = bucket.Len()
		var currentIndex uint
		for i := 0; i < len; i++ {
			item := bucket.items[currentIndex]

			if item.CanTimeout {
				currentTime := ftime.Now()
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
						return false
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
		return true
	})

	// Iterate through the buckets and remove empty buckets
	cpq.buckets.Range(func(key uint, bucket *priorityQueue[T]) bool {
		if bucket.Len() == 0 {
			cpq.bpq.Delete(key)
		}
		return true
	})

	if err != nil {
		return removed, escalated, err
	}

	// This is a very basic but fast algorithm that iterates from the front to the back of the queue.
	// If the item can escalate and has reached its ticker, then we check if the last item was escalated,
	// and that we are not first in the queue. This strategy means that messages can only push up the queue,
	// if other messages are also not being prioritized. In this model, messages not being escalated,
	// can be impacted by other high priority messages allowing for fairly complex queue strategies.
	// I think in the future this can allow for more advanced features but seems fine for now.
	cpq.buckets.Range(func(key uint, bucket *priorityQueue[T]) bool {
		var lastItemWasEscalated bool
		var len = bucket.Len()

		for i := 0; i < len; i++ {
			item := bucket.items[i]

			if item.ShouldEscalate {
				currentTime := ftime.Now()
				if currentTime.Sub(item.LastEscalated) > item.EscalationRate {

					if !lastItemWasEscalated && i != 0 {
						item.LastEscalated = currentTime
						bucket.UpdatePriority(item, i-1)
						escalated++
					}
					// We don't need to update lastItemWasEscalated here because we just swapped
					// the current cursor index, with cursor index - 1. The previous index must have
					// not been escalated so we don't need to update lastItemWasEscalated
				}
			} else {
				lastItemWasEscalated = false
			}
		}

		return true
	})

	return removed, escalated, nil
}
