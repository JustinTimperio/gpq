package gpq

import (
	"sync"
	"sync/atomic"
)

type Bucket struct {
	BucketID   int64
	Prev, Next *Bucket
}

// Bucket priority queue implementation.
// This is used to keep track of non-empty buckets in the GPQ
// This is a combination of a HashSet, doubly linked list, and a priority queue
// to allow for O(1) removal of buckets and removal of items from the buckets
// and O(1) addition of buckets and addition of items to the buckets
type BucketPriorityQueue struct {
	ActiveBuckets int64
	BucketIDs     map[int64]*Bucket
	First, Last   *Bucket
	mutex         *sync.Mutex
}

// NewBucketPriorityQueue creates a new BucketPriorityQueue
func NewBucketPriorityQueue() *BucketPriorityQueue {
	return &BucketPriorityQueue{
		ActiveBuckets: 0,
		BucketIDs:     make(map[int64]*Bucket),
		mutex:         &sync.Mutex{},
	}
}

func (bpq *BucketPriorityQueue) Len() *int64 {
	return &bpq.ActiveBuckets
}

func (bpq *BucketPriorityQueue) Peek() (bucketID int64, exists bool) {
	bpq.mutex.Lock()
	defer bpq.mutex.Unlock()

	if bpq.First == nil {
		return 0, false
	}
	return bpq.First.BucketID, true
}

func (bpq *BucketPriorityQueue) Add(bucketID int64) {
	bpq.mutex.Lock()
	defer bpq.mutex.Unlock()

	// If the bucket already exists, return
	if _, exists := bpq.BucketIDs[bucketID]; exists {
		return
	}

	// Create a new bucket
	newBucket := &Bucket{BucketID: bucketID}

	// If the queue is empty, set the new bucket as the first and last
	if bpq.First == nil {
		bpq.First = newBucket
		bpq.Last = newBucket

	} else {
		// Find the correct position to insert the new bucket
		current := bpq.First
		for current != nil && current.BucketID < bucketID {
			current = current.Next
		}

		if current == bpq.First { // Insert the new bucket at the beginning
			newBucket.Next = bpq.First
			bpq.First.Prev = newBucket
			bpq.First = newBucket
		} else if current == nil { // Insert the new bucket at the end
			newBucket.Prev = bpq.Last
			bpq.Last.Next = newBucket
			bpq.Last = newBucket
		} else { // Insert the new bucket in the middle
			newBucket.Prev = current.Prev
			newBucket.Next = current
			current.Prev.Next = newBucket
			current.Prev = newBucket
		}
	}

	bpq.BucketIDs[bucketID] = newBucket
	atomic.AddInt64(&bpq.ActiveBuckets, 1)
}

func (bpq *BucketPriorityQueue) Remove(bucketID int64) {
	bpq.mutex.Lock()
	defer bpq.mutex.Unlock()

	// If the bucket does not exist, return
	bucket, exists := bpq.BucketIDs[bucketID]
	if !exists {
		return
	}

	// Update the first and last pointers if necessary
	if bucket.Prev != nil {
		bucket.Prev.Next = bucket.Next
	} else {
		bpq.First = bucket.Next
	}
	if bucket.Next != nil {
		bucket.Next.Prev = bucket.Prev
	} else {
		bpq.Last = bucket.Prev
	}

	// Remove the bucket from the map and decrement the active bucket count
	delete(bpq.BucketIDs, bucketID)
	atomic.AddInt64(&bpq.ActiveBuckets, -1)
}
