package gpq

import (
	"sync"

	"github.com/cornelk/hashmap"
)

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
	Buckets     *hashmap.Map[int64, bool]
	BucketIDs   *hashmap.Map[int64, *Bucket[d]]
	First, Last *Bucket[d]
	Mutex       *sync.Mutex
}

func NewBucketPriorityQueue[d any]() *BucketPriorityQueue[d] {
	return &BucketPriorityQueue[d]{
		Buckets:   hashmap.New[int64, bool](),
		BucketIDs: hashmap.New[int64, *Bucket[d]](),
		Mutex:     &sync.Mutex{},
	}
}

func (pq *BucketPriorityQueue[d]) Len() int {
	return int(pq.Buckets.Len())
}

func (pq *BucketPriorityQueue[d]) Peek() (bucketID int64, exists bool) {
	if pq.First == nil {
		return 0, false
	}
	return pq.First.BucketID, true
}

func (pq *BucketPriorityQueue[d]) Add(bucketID int64) {

	if _, exists := pq.BucketIDs.Get(bucketID); exists {
		return
	}

	pq.Mutex.Lock()
	defer pq.Mutex.Unlock()

	newBucket := &Bucket[d]{BucketID: bucketID}
	pq.Buckets.Set(bucketID, true)
	pq.BucketIDs.Set(bucketID, newBucket)

	if pq.First == nil {
		pq.First = newBucket
		pq.Last = newBucket
	} else {
		current := pq.First
		for current != nil && current.BucketID < bucketID {
			current = current.Next
		}
		if current == pq.First {
			newBucket.Next = pq.First
			pq.First.Prev = newBucket
			pq.First = newBucket
		} else if current == nil {
			newBucket.Prev = pq.Last
			pq.Last.Next = newBucket
			pq.Last = newBucket
		} else {
			newBucket.Prev = current.Prev
			newBucket.Next = current
			current.Prev.Next = newBucket
			current.Prev = newBucket
		}
	}
}

func (pq *BucketPriorityQueue[d]) Remove(bucketID int64) {
	pq.Mutex.Lock()
	defer pq.Mutex.Unlock()
	bucket, exists := pq.BucketIDs.Get(bucketID)
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
	pq.Buckets.Del(bucketID)
	pq.BucketIDs.Del(bucketID)
}
