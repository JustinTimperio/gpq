package queues

import (
	"errors"

	"github.com/JustinTimperio/gpq/queues/gheap"
	"github.com/JustinTimperio/gpq/schema"
)

// NewCorePriorityQueue creates a new CorePriorityQueue
func newPriorityQueue[T any]() priorityQueue[T] {
	pq := priorityQueue[T]{
		items: make([]*schema.Item[T], 0),
	}
	gheap.Init[T](&pq)

	return pq
}

// priorityQueue implements heap.Interface and holds Items.
type priorityQueue[T any] struct {
	items []*schema.Item[T]
}

// Len is used to get the length of the heap
// It is needed to implement the heap.Interface
func (pq *priorityQueue[T]) Len() int {
	return len(pq.items)
}

// Less is used to compare the priority of two items
// It is needed to implement the heap.Interface
func (pq *priorityQueue[T]) Less(i, j int) bool {
	return pq.items[i].InternalPriority < pq.items[j].InternalPriority
}

// Swap is used to swap two items in the heap
// It is needed to implement the heap.Interface
func (pq *priorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}

// EnQueue adds an item to the end of the heap
func (pq *priorityQueue[T]) Enqueue(data *schema.Item[T]) {
	n := len(pq.items)
	data.InternalPriority = n
	item := data
	item.Index = n
	pq.items = append(pq.items, item)
}

// DeQueue removes the first item from the heap
func (pq *priorityQueue[T]) Dequeue() (data *schema.Item[T], err error) {
	if len(pq.items) == 0 {
		return data, errors.New("Internal Priority Queue Error: No items found in the queue")
	}

	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // don't stop the GC from reclaiming the item eventually
	pq.items = old[0 : n-1]

	return item, nil
}

// UpdatePriority modifies the priority of an Item in the queue.
func (pq *priorityQueue[T]) UpdatePriority(item *schema.Item[T], newPriority int) {
	item.InternalPriority = newPriority
	gheap.Prioritize[T](pq, item.Index)
}
