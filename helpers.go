package gpq

import (
	"sync"

	"github.com/JustinTimperio/gpq/disk"
	"github.com/JustinTimperio/gpq/schema"
)

type batchHandler[T any] struct {
	mux            *sync.Mutex
	syncedBatches  map[uint]bool
	deletedBatches map[uint]bool
	diskCache      *disk.Disk[T]
}

func newBatchHandler[T any](diskCache *disk.Disk[T]) *batchHandler[T] {
	return &batchHandler[T]{
		mux:            &sync.Mutex{},
		syncedBatches:  make(map[uint]bool),
		deletedBatches: make(map[uint]bool),
		diskCache:      diskCache,
	}
}

func (bh *batchHandler[T]) processBatch(batch []*schema.Item[T], batchNumber uint) {
	bh.mux.Lock()
	defer bh.mux.Unlock()

	deleted, ok := bh.deletedBatches[batchNumber]
	if !ok || (ok && !deleted) {
		bh.diskCache.ProcessBatch(batch)
	}

	bh.syncedBatches[batchNumber] = true
	bh.deletedBatches[batchNumber] = false
}

func (bh *batchHandler[T]) deleteBatch(batch []*schema.DeleteMessage, batchNumber uint, wasRestored bool) {
	bh.mux.Lock()
	defer bh.mux.Unlock()

	if wasRestored {
		bh.diskCache.DeleteBatch(batch)
		return
	}

	bh.syncedBatches[batchNumber] = false
	bh.deletedBatches[batchNumber] = true

	if _, ok := bh.syncedBatches[batchNumber]; ok {
		bh.diskCache.DeleteBatch(batch)
		return
	}

}

type batchCounter struct {
	mux          *sync.Mutex
	batchNumber  uint
	batchCounter uint
	batchSize    uint
}

func newBatchCounter(batchSize uint) *batchCounter {
	return &batchCounter{
		mux:          &sync.Mutex{},
		batchNumber:  0,
		batchCounter: 0,
		batchSize:    batchSize,
	}
}

func (bc *batchCounter) increment() (batchNumber uint) {
	bc.mux.Lock()
	defer bc.mux.Unlock()

	if (bc.batchCounter % bc.batchSize) == 0 {
		bc.batchNumber++
	}
	bc.batchCounter++
	return bc.batchNumber
}
