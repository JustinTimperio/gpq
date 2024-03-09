// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package heap provides heap operations for any type that implements
// heap.Interface. A heap is a tree with the property that each node is the
// minimum-valued node in its subtree.
//
// The minimum element in the tree is the root, at index 0.
//
// This package has be modified to allow for generics - Justin
package gheap

import (
	"errors"
	"sort"
)

// The Interface type describes the requirements
// for a type using the routines in this package.
// Any type that implements it may be used as a
// min-heap with the following invariants (established after
// [Init] has been called or if the data is empty or sorted):
//
//	!h.Less(j, i) for 0 <= i < h.Len() and 2*i+1 <= j <= 2*i+2 and j < h.Len()
//
// Note that [EnQueue] and [DeQueue] in this interface are for package heap's
// implementation to call. To add and remove things from the heap,
// use [heap.EnQueue] and [heap.DeQueue].
type Heap[T any, S any] interface {
	sort.Interface
	EnQueue(x S)                                // add x as element Len()
	DeQueue() (uint64, []byte, int64, T, error) // remove and return element Len() - 1.
	NoLockDeQueue()
}

// Init establishes the heap invariants required by the other routines in this package.
// Init is idempotent with respect to the heap invariants
// and may be called whenever the heap invariants may have been invalidated.
// The complexity is O(n) where n = h.Len().
func Init[T any, S any](h Heap[T, S]) {
	// heapify
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

// EnQueue EnQueues the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func EnQueue[T any, S any](h Heap[T, S], x S) {
	h.EnQueue(x)
	up(h, h.Len()-1)
}

// DeQueue removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// DeQueue is equivalent to [Remove](h, 0).
func DeQueue[T any, S any](h Heap[T, S]) (batchNumber uint64, diskUUID []byte, priority int64, data T, err error) {
	if h.Len() == 0 {
		return 0, nil, -1, data, errors.New("No items in the queue")
	}
	n := h.Len() - 1
	h.Swap(0, n)
	down(h, 0, n)
	return h.DeQueue()
}

func NoLockDeQueue[T any, S any](h Heap[T, S]) {
	if h.Len() == 0 {
		return
	}
	n := h.Len() - 1
	h.Swap(0, n)
	down(h, 0, n)
	h.NoLockDeQueue()
	return
}

// Remove removes and returns the element at index i from the heap.
// The complexity is O(log n) where n = h.Len().
func Remove[T any, S any](h Heap[T, S], i int) {
	n := h.Len() - 1
	if n != i {
		h.Swap(i, n)
		if !down(h, i, n) {
			up(h, i)
		}
	}
	h.NoLockDeQueue()
	return
}

// Prioritize re-establishes the heap ordering after the element at index i has changed its value.
// Changing the value of the element at index i and then calling Prioritize is equivalent to,
// but less expensive than, calling [Remove](h, i) followed by a EnQueue of the new value.
// The complexity is O(log n) where n = h.Len().
func Prioritize[T any, S any](h Heap[T, S], i int) {
	if !down(h, i, h.Len()) {
		up(h, i)
	}
}

func up[T any, S any](h Heap[T, S], j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down[T any, S any](h Heap[T, S], i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
