package gpq

import (
	"errors"
	"sync/atomic"
	"unsafe"

	"github.com/JustinTimperio/gpq/schema"
)

type node[T any] struct {
	value *schema.Item[T]
	next  unsafe.Pointer // *node
}

type LockFreeQueue[T any] struct {
	head unsafe.Pointer // *node
	tail unsafe.Pointer // *node
}

func NewLockFreeQueue[T any]() *LockFreeQueue[T] {
	n := unsafe.Pointer(&node[T]{})
	return &LockFreeQueue[T]{head: n, tail: n}
}

func (q *LockFreeQueue[T]) EnQueue(value schema.Item[T]) {
	n := &node[T]{value: &value}
	for {
		tail := load[T](&q.tail)
		next := load[T](&tail.next)
		if tail == load[T](&q.tail) { // are tail and next consistent?
			if next == nil {
				if cas(&tail.next, next, n) {
					cas(&q.tail, tail, n) // enqueue is done.  try to swing tail to the inserted node
					return
				}
			} else { // tail was not pointing to the last node
				// try to swing Tail to the next node
				cas(&q.tail, tail, next)
			}
		}
	}
}

func (q *LockFreeQueue[T]) DeQueue() (schema.Item[T], error) {
	for {
		head := load[T](&q.head)
		tail := load[T](&q.tail)
		next := load[T](&head.next)
		if head == load[T](&q.head) { // are head, tail, and next consistent?
			if head == tail { // is queue empty or tail falling behind?
				if next == nil { // is queue empty?
					return schema.Item[T]{}, errors.New("Queue is empty")
				}
				// tail is falling behind.  try to advance it
				cas(&q.tail, tail, next)
			} else {
				// read value before CAS otherwise another dequeue might free the next node
				value := next.value
				// try to swing Head to the next node
				if cas(&q.head, head, next) {
					return *value, nil
				}
			}
		}
	}
}

func (q *LockFreeQueue[T]) Peek() (schema.Item[T], bool) {
	head := load[T](&q.head)
	tail := load[T](&q.tail)
	next := load[T](&head.next)
	if head == load[T](&q.head) { // are head, tail, and next consistent?
		if head == tail { // is queue empty or tail falling behind?
			if next == nil { // is queue empty?
				return schema.Item[T]{}, false
			}
			return *next.value, true
		}
		return *next.value, true
	}
	return schema.Item[T]{}, false
}

func (q *LockFreeQueue[T]) ReadPointers() []*node[T] {
	var pointers []*node[T]
	head := load[T](&q.head)
	next := load[T](&head.next)
	if head == load[T](&q.head) { // are head, tail, and next consistent?
		for next != nil {
			pointers = append(pointers, next)
			next = load[T](&next.next)
		}
	}
	return pointers
}

func (q *LockFreeQueue[T]) UpdatePriority(index int, value schema.Item[T]) bool {
	pointers := q.ReadPointers()
	if index >= 0 && index < len(pointers) {
		node := pointers[index]
		node.value = &value
		return true
	}
	return false
}

func (q *LockFreeQueue[T]) Remove(index int) bool {
	pointers := q.ReadPointers()
	if index >= 0 && index < len(pointers) {
		prev := load[T](&q.head)
		curr := load[T](&prev.next)
		for i := 0; i < index; i++ {
			prev = curr
			curr = load[T](&curr.next)
		}
		next := load[T](&curr.next)
		if cas[T](&prev.next, curr, next) {
			return true
		}
	}
	return false
}

func load[T any](p *unsafe.Pointer) *node[T] {
	return (*node[T])(atomic.LoadPointer(p))
}

func store[T any](p *unsafe.Pointer, n *node[T]) {
	atomic.StorePointer(p, unsafe.Pointer(n))
}

func cas[T any](p *unsafe.Pointer, old, new *node[T]) bool {
	return atomic.CompareAndSwapPointer(p, unsafe.Pointer(old), unsafe.Pointer(new))
}
