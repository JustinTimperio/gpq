package gpq

import "sync"

// Bucket priority queue implementation.
// This is used to keep track of non-empty buckets in the GPQ
// This is a combination of a HashSet, doubly linked list, and a priority queue
// to allow for O(1) removal of buckets and O(1) removal of items from the buckets
// and O(1) addition of buckets and O(1) addition of items to the buckets
type Bucket[d any] struct {
	BucketID   int64
	Prev, Next *Bucket[d]
}

type BucketPriorityQueue[d any] struct {
	Mutex       *sync.Mutex
	Buckets     map[*Bucket[d]]bool
	BucketIDs   map[int64]*Bucket[d]
	First, Last *Bucket[d]
}

func NewBucketPriorityQueue[d any]() *BucketPriorityQueue[d] {
	return &BucketPriorityQueue[d]{
		Buckets:   make(map[*Bucket[d]]bool),
		BucketIDs: make(map[int64]*Bucket[d]),
		Mutex:     &sync.Mutex{},
	}
}

func (pq *BucketPriorityQueue[d]) Len() int {
	return len(pq.Buckets)
}

func (pq *BucketPriorityQueue[d]) Peek() (bucketID int64, exists bool) {
	if pq.First == nil {
		return 0, false
	}
	return pq.First.BucketID, true
}

func (pq *BucketPriorityQueue[d]) Add(bucketID int64) {
	pq.Mutex.Lock()
	defer pq.Mutex.Unlock()

	if _, exists := pq.BucketIDs[bucketID]; exists {
		return
	}
	bucket := &Bucket[d]{BucketID: bucketID}
	pq.Buckets[bucket] = true
	pq.BucketIDs[bucketID] = bucket
	if pq.Last != nil {
		pq.Last.Next = bucket
		bucket.Prev = pq.Last
	}
	pq.Last = bucket
	if pq.First == nil {
		pq.First = bucket
	}
}

func (pq *BucketPriorityQueue[d]) Remove(bucketID int64) {
	pq.Mutex.Lock()
	defer pq.Mutex.Unlock()
	bucket, exists := pq.BucketIDs[bucketID]
	if !exists {
		return
	}
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
	delete(pq.Buckets, bucket)
	delete(pq.BucketIDs, bucketID)
}
