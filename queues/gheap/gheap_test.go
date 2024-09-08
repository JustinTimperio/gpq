package gheap_test

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestStdHeap(t *testing.T) {

	var itemsArr []Item
	var counter int

	items := map[string]int{
		"banana": 3,
		"apple":  2,
		"pear":   4,
		"orange": 1,
		"kiwi":   5,
		"peach":  6,
	}

	for value, priority := range items {
		x := Item{
			value:    value,
			priority: priority,
			index:    counter,
		}
		itemsArr = append(itemsArr, x)
		counter++
	}

	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	for _, item := range itemsArr {
		heap.Push(&pq, &item)
		fmt.Println("Pushed item:", item, "New length:", pq.Len())
	}

	for _, item := range itemsArr {
		// Find the item in pq
		var i int
		for _, v := range pq {
			if v.value == item.value {
				i = v.index
				break
			}
		}
		fmt.Println(item)
		heap.Remove(&pq, i)
	}
}

// An Item is something we manage in a priority queue.
type Item struct {
	value    string // The value of the item; arbitrary.
	priority int    // The priority of the item in the queue.
	index    int    // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value string, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
