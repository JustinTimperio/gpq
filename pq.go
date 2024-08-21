package gpq

import (
	"errors"
	"sync"

	"github.com/JustinTimperio/gpq/gheap"
	"github.com/JustinTimperio/gpq/schema"
)

// CorePriorityQueue implements heap.Interface and holds Items.
type CorePriorityQueue[T any] struct {
	items []*schema.Item[T]
	mutex *sync.RWMutex
}

// NewCorePriorityQueue creates a new CorePriorityQueue
func NewCorePriorityQueue[T any]() CorePriorityQueue[T] {
	return CorePriorityQueue[T]{
		items: make([]*schema.Item[T], 0),
		mutex: &sync.RWMutex{},
	}
}

// Len is used to get the length of the heap
// It is needed to implement the heap.Interface
func (pq *CorePriorityQueue[T]) Len() int {
	return len(pq.items)
}

// Less is used to compare the priority of two items
// It is needed to implement the heap.Interface
func (pq *CorePriorityQueue[T]) Less(i, j int) bool {
	return pq.items[i].Priority > pq.items[j].Priority
}

// Swap is used to swap two items in the heap
// It is needed to implement the heap.Interface
func (pq *CorePriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}

// EnQueue adds an item to the heap at the end of the array
func (pq *CorePriorityQueue[T]) EnQueue(data schema.Item[T]) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	// Add the item to the heap
	n := len(pq.items)
	item := data
	item.Index = n
	pq.items = append(pq.items, &item)
}

// DeQueue removes the first item from the heap
func (pq *CorePriorityQueue[T]) DeQueue() (wasRecoverd bool, batchNumber uint64, diskUUID []byte, priority int64, data T, err error) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if len(pq.items) == 0 {
		return false, 0, nil, -1, data, errors.New("Core Priority Queue Error: No items found in the queue")
	}

	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	pq.items = old[0 : n-1]

	return item.WasRestored, item.BatchNumber, item.DiskUUID, item.Priority, item.Data, nil
}

// Peek returns the first item in the heap without removing it
func (pq *CorePriorityQueue[T]) Peek() (data T, err error) {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()
	if len(pq.items) == 0 {
		return data, errors.New("No items in the queue")
	}
	return pq.items[0].Data, nil
}

// Exposes the raw pointers to the items in the queue so that reprioritization can be done
func (pq *CorePriorityQueue[T]) ReadPointers() []*schema.Item[T] {
	return pq.items
}

// UpdatePriority modifies the priority of an Item in the queue.
func (pq *CorePriorityQueue[T]) UpdatePriority(item *schema.Item[T], priority int64) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	item.Priority = priority
	gheap.Prioritize[T](pq, item.Index)
}

// Remove removes an item from the queue
func (pq *CorePriorityQueue[T]) Remove(item *schema.Item[T]) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	gheap.Remove[T](pq, item.Index)
}

// NoLockDeQueue removes the first item from the heap without locking the queue
// This is used for nested calls to avoid deadlocks
func (pq *CorePriorityQueue[T]) NoLockDeQueue() {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	pq.items = old[0 : n-1]
}
