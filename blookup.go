package gpq

import (
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
	BucketIDs   *hashmap.Map[int64, *Bucket[d]]
	First, Last *Bucket[d]
}

func NewBucketPriorityQueue[d any]() *BucketPriorityQueue[d] {
	return &BucketPriorityQueue[d]{
		BucketIDs: hashmap.New[int64, *Bucket[d]](),
	}
}

func (pq *BucketPriorityQueue[d]) Len() int {
	return int(pq.BucketIDs.Len())
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

	newBucket := &Bucket[d]{BucketID: bucketID}
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
	pq.BucketIDs.Del(bucketID)
}

func (pq *BucketPriorityQueue[d]) Contains(bucketID int64) bool {
	_, exists := pq.BucketIDs.Get(bucketID)
	return exists
}
