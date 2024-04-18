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
	ActiveBuckets  int64
	BucketIDs      map[int64]*Bucket
	First, Last    *Bucket
	LastRemoved    int64
	ObjectsInQueue uint64
	mutex          *sync.Mutex
}

// NewBucketPriorityQueue creates a new BucketPriorityQueue
func NewBucketPriorityQueue() *BucketPriorityQueue {
	return &BucketPriorityQueue{
		ActiveBuckets:  0,
		ObjectsInQueue: 0,
		BucketIDs:      make(map[int64]*Bucket),
		mutex:          &sync.Mutex{},
	}
}

func (pq *BucketPriorityQueue) Len() *int64 {
	return &pq.ActiveBuckets
}

func (pq *BucketPriorityQueue) Peek() (bucketID int64, exists bool) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if pq.First == nil {
		return 0, false
	}
	return pq.First.BucketID, true
}

func (pq *BucketPriorityQueue) Add(bucketID int64) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	// If the bucket already exists, return
	if _, exists := pq.BucketIDs[bucketID]; exists {
		return
	}

	// Create a new bucket
	newBucket := &Bucket{BucketID: bucketID}

	// If the queue is empty, set the new bucket as the first and last
	if pq.First == nil {
		pq.First = newBucket
		pq.Last = newBucket

	} else {
		// Find the correct position to insert the new bucket
		current := pq.First
		for current != nil && current.BucketID < bucketID {
			current = current.Next
		}

		if current == pq.First { // Insert the new bucket at the beginning
			newBucket.Next = pq.First
			pq.First.Prev = newBucket
			pq.First = newBucket
		} else if current == nil { // Insert the new bucket at the end
			newBucket.Prev = pq.Last
			pq.Last.Next = newBucket
			pq.Last = newBucket
		} else { // Insert the new bucket in the middle
			newBucket.Prev = current.Prev
			newBucket.Next = current
			current.Prev.Next = newBucket
			current.Prev = newBucket
		}
	}

	pq.BucketIDs[bucketID] = newBucket
	atomic.AddInt64(&pq.ActiveBuckets, 1)
}

func (pq *BucketPriorityQueue) Remove(bucketID int64) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	// If the bucket does not exist, return
	bucket, exists := pq.BucketIDs[bucketID]
	if !exists {
		return
	}

	// Update the first and last pointers if necessary
	if bucket.Prev != nil {
		bucket.Prev.Next = bucket.Next
	} else {
		pq.First = bucket.Next
	}
	if bucket.Next != nil {
		bucket.Next.Prev = bucket.Prev
	} else {
		pq.Last = bucket.Prev
	}

	// Remove the bucket from the map and decrement the active bucket count
	delete(pq.BucketIDs, bucketID)
	atomic.AddInt64(&pq.ActiveBuckets, -1)
	atomic.StoreInt64(&pq.LastRemoved, bucketID)
}

func (pq *BucketPriorityQueue) Contains(bucketID int64) bool {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	_, exists := pq.BucketIDs[bucketID]
	return exists
}
