package gpq

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/JustinTimperio/gpq/gheap"
	"github.com/JustinTimperio/gpq/schema"
)

// A PriorityQueue implements heap.Interface and holds Items.
type CorePriorityQueue[T any] struct {
	items []*schema.Item[T]
	mutex *sync.RWMutex
	bpq   *BucketPriorityQueue
}

func NewCorePriorityQueue[T any](bpq *BucketPriorityQueue) CorePriorityQueue[T] {
	return CorePriorityQueue[T]{
		items: make([]*schema.Item[T], 0),
		mutex: &sync.RWMutex{},
		bpq:   bpq,
	}
}

// Len is used to get the length of the heap
// It is needed to implement the heap.Interface
func (pq CorePriorityQueue[T]) Len() int {
	return len(pq.items)
}

// Less is used to compare the priority of two items
// It is needed to implement the heap.Interface
func (pq CorePriorityQueue[T]) Less(i, j int) bool {
	return pq.items[i].Priority > pq.items[j].Priority
}

// Swap is used to swap two items in the heap
// It is needed to implement the heap.Interface
func (pq CorePriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}

// EnQueue adds an item to the heap and the end of the array
func (pq *CorePriorityQueue[T]) EnQueue(data schema.Item[T]) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	n := len(pq.items)
	item := data
	item.Index = n
	pq.items = append(pq.items, &item)

	atomic.AddUint64(&pq.bpq.ObjectsInQueue, 1)
	if !pq.bpq.Contains(item.Priority) {
		pq.bpq.Add(item.Priority)
	}
}

// DeQueue removes the first item from the heap
func (pq *CorePriorityQueue[T]) DeQueue() (batchNumber uint64, diskUUID []byte, priority int64, data T, err error) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if len(pq.items) == 0 {
		return 0, nil, -1, data, errors.New("Core Priority Queue Error: No items found in the queue")
	}

	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	pq.items = old[0 : n-1]

	// Check if the bucket is now empty
	if len(pq.items) == 0 {
		pq.bpq.Remove(item.Priority)
	}

	atomic.AddUint64(&pq.bpq.ObjectsInQueue, ^uint64(0))
	return item.BatchNumber, item.DiskUUID, item.Priority, item.Data, nil
}

func (pq CorePriorityQueue[T]) Peek() (data T, err error) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	if len(pq.items) == 0 {
		return data, errors.New("No items in the queue")
	}
	return pq.items[0].Data, nil
}

func (pq CorePriorityQueue[T]) ReadPointers() []*schema.Item[T] {
	return pq.items
}

// UpdatePriority modifies the priority of an Item in the queue.
func (pq *CorePriorityQueue[T]) UpdatePriority(item *schema.Item[T], priority int64) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	item.Priority = priority
	gheap.Prioritize[T](pq, item.Index)
}

func (pq *CorePriorityQueue[T]) Remove(item *schema.Item[T]) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	gheap.Remove[T](pq, item.Index)
}

func (pq *CorePriorityQueue[T]) NoLockDeQueue() {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	pq.items = old[0 : n-1]

	// Check if the bucket is now empty
	if len(pq.items) == 0 {
		pq.bpq.Remove(item.Priority)
	}

	atomic.AddUint64(&pq.bpq.ObjectsInQueue, ^uint64(0))
}
