package gpq

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq/schema"
)

// GPQ is a priority queue that supports priority escalation
type GPQ[d any] struct {
	// TotalLen is the total length of the GPQ
	TotalLen uint64
	// Buckets is a map of priority buckets
	Buckets map[int64]*CorePriorityQueue[d]
	// MutexMap is a map of mutexes for each priority bucket
	BucketMutexMap map[int64]*sync.Mutex
	// LenMap is a map of the length of each priority bucket
	BucketLenMap map[int64]uint64
	// NonEmptyBuckets is a priority queue of non-empty buckets
	NonEmptyBuckets *BucketPriorityQueue[d]
}

// NewGPQ creates a new GPQ with the given number of buckets
// The number of buckets is the number of priority levels you want to support
// You must provide the number of buckets ahead of time and all priorities you submit
// must be within the range of 0 to NumOfBuckets
func NewGPQ[d any](NumOfBuckets int) *GPQ[d] {
	bp := NewBucketPriorityQueue[d]()

	gpq := &GPQ[d]{
		Buckets:         make(map[int64]*CorePriorityQueue[d]),
		BucketLenMap:    make(map[int64]uint64),
		BucketMutexMap:  make(map[int64]*sync.Mutex),
		NonEmptyBuckets: bp,
	}

	for i := 0; i < NumOfBuckets; i++ {
		pq := make(CorePriorityQueue[d], 0)
		gpq.Buckets[int64(i)] = &pq
		gpq.BucketMutexMap[int64(i)] = &sync.Mutex{}
		gpq.BucketLenMap[int64(i)] = 0
	}

	return gpq
}

// EnQueue adds an item to the GPQ
// The priorityBucket is the priority level of the item
// The escalationRate is the amount of time before the item is escalated to the next priority level
// The data is the data you want to store in the GPQ item
func (g *GPQ[d]) EnQueue(data d, priorityBucket int64, escalationRate time.Duration) error {

	if priorityBucket > int64(len(g.Buckets)) {
		return errors.New("Priority bucket does not exist")
	}

	// Create the item
	obj := schema.Item[d]{
		Data:           data,
		Priority:       priorityBucket,
		EscalationRate: escalationRate,
		SubmittedAt:    time.Now(),
		LastEscalated:  time.Now(),
	}

	mutex, ok := g.BucketMutexMap[priorityBucket]
	if !ok {
		return errors.New("Mutex does not exist")
	}

	count := g.BucketLenMap[priorityBucket]
	if !ok {
		return errors.New("Bucket length does not exist")
	}

	pq, ok := g.Buckets[priorityBucket]
	if !ok {
		return errors.New("Priority bucket does not exist")
	}

	// Enqueue the item
	mutex.Lock()
	defer mutex.Unlock()
	pq.EnQueue(obj)
	g.NonEmptyBuckets.Add(priorityBucket)

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
	for g.NonEmptyBuckets.Len() > 0 {
		bucketID, exists := g.NonEmptyBuckets.Peek()
		if !exists {
			return -1, data, errors.New("No items in the queue")
		}

		mutex, ok := g.BucketMutexMap[bucketID]
		if !ok {
			return -1, data, errors.New("Mutex does not exist")
		}

		count, ok := g.BucketLenMap[bucketID]
		if !ok {
			return -1, data, errors.New("Bucket length does not exist")
		}

		pq, ok := g.Buckets[bucketID]
		if !ok {
			return -1, data, errors.New("Priority bucket does not exist")
		}

		// Lock the priority bucket
		mutex.Lock()
		defer mutex.Unlock()
		// Dequeue the item
		priority, item, err := pq.DeQueue()
		if err != nil {
			g.NonEmptyBuckets.Remove(bucketID)
			continue
		}

		// Decrement the length of the priority bucket and the total length
		// ^uint64(0) is the same as -1 (idk some cursed ass fuckery)
		atomic.AddUint64(&count, ^uint64(0))
		atomic.AddUint64(&g.TotalLen, ^uint64(0))
		return priority, item, nil
	}

	return -1, data, errors.New("No items in the queue")

}

// Prioritize is a method of the GPQ type that prioritizes items within a heap.
// It iterates over each bucket in the GPQ, locks the corresponding mutex, and checks if there are items to prioritize.
// If there are items, it calculates the number of durations that have passed since the last escalation and updates the priority accordingly.
// The method uses goroutines to process each bucket concurrently, improving performance.
// It returns an error if any of the required data structures are missing or if there are no items to prioritize.
func (g *GPQ[d]) Prioritize() (uint64, []error) {
	wg := sync.WaitGroup{}
	errCh := make(chan error, len(g.Buckets)) // Channel to collect errors
	var shuffledItemTotal uint64

	for i := 0; i < len(g.Buckets); i++ {
		wg.Add(1)

		go func(bucketID int64) {
			defer wg.Done()

			// Try and Lock
			mutex, ok := g.BucketMutexMap[bucketID]
			if !ok {
				errCh <- errors.New("Mutex does not exist")
				return
			}

			pq, ok := g.Buckets[bucketID]
			if !ok {
				errCh <- errors.New("Priority bucket does not exist")
				return
			}

			mutex.Lock()
			defer mutex.Unlock()
			_, err := pq.Peek()
			if err != nil {
				errCh <- errors.New("No items to prioritize in heap: " + fmt.Sprintf("%d", bucketID))
				return
			}

			pointers := pq.ReadPointers()
			evalTime := time.Now()
			for _, pointer := range pointers {

				// Calculate the number of durations that fit between evalTime and pointer.SubmittedAt
				duration := int(math.Abs(float64(pointer.LastEscalated.Sub(evalTime))))
				numDurations := duration / int(pointer.EscalationRate)

				// If the number of durations is greater than 0, escalate the priority
				if numDurations > 0 {
					pointer.Priority = pointer.Priority - int64(numDurations)
					if pointer.Priority < 0 {
						pointer.Priority = 0
					}
					pointer.LastEscalated = evalTime
					pq.UpdatePriority(pointer, pointer.Priority)
					atomic.AddUint64(&shuffledItemTotal, 1)
				}

			}
		}(int64(i))
	}

	go func() {
		wg.Wait()
		close(errCh) // Close the error channel after all goroutines have completed
	}()

	var errs []error
	// Collect errors from the error channel
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return shuffledItemTotal, errs
}

func (g *GPQ[d]) Stats() (uint64, map[int64]uint64) {

	return g.TotalLen, g.BucketLenMap
}
