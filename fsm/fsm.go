package fsm

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq/schema"
	"github.com/cornelk/hashmap"
	"github.com/dgraph-io/badger/v4"
	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/hashicorp/raft"
)

type Command string

const (
	EnQueue Command = "enqueue"
	DeQueue Command = "dequeue"
	Peek    Command = "peek"
)

type FSM[d any] struct {
	// Core
	db                      *badger.DB
	buckets                 *hashmap.Map[int64, *CorePriorityQueue[d]]
	bucketPrioritizeLockMap *hashmap.Map[int64, *sync.Mutex]
	nonEmptyBuckets         *BucketPriorityQueue
	// Lazy disk settings
	diskCache          bool
	lazyDiskSendChan   chan schema.LazyMessageQueueItem
	lazyDiskDeleteChan chan schema.LazyMessageQueueItem
	// Batch Utils
	allBatchesSynced *sync.WaitGroup
	syncedBatches    *sync.Map
	batchNumber      uint64
	batchCounter     uint64
	batchMutex       *sync.Mutex
	batchSize        uint64
	// Active DB Sessions
	activeDBSessions *sync.WaitGroup
	// debug
	sendBatchesProcessed    uint64
	sendMessagesProcessed   uint64
	deleteBatchesProcessed  uint64
	deleteMessagesProcessed uint64
	txnDeleteProcessed      uint64
	txnSetProcessed         uint64
}

type CommandPayload[d any] struct {
	Operation     Command
	LazyOperation bool
	Data          schema.Item[d]
}

type ApplyResponse[d any] struct {
	Success bool
	Err     error
	Data    d
	// For Dequeue
	Priority int64
}

func NewFSM[d any](options schema.GPQOptions) (*FSM[d], error) {
	opts := badger.DefaultOptions(options.DiskCachePath)
	opts.Logger = nil
	if options.Compression {
		opts.Compression = badgerOptions.ZSTD
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.New("Error opening disk cache: " + err.Error())
	}

	fsm := &FSM[d]{
		db:                      db,
		diskCache:               options.DiskCache,
		lazyDiskSendChan:        make(chan schema.LazyMessageQueueItem),
		lazyDiskDeleteChan:      make(chan schema.LazyMessageQueueItem),
		allBatchesSynced:        &sync.WaitGroup{},
		syncedBatches:           &sync.Map{},
		batchNumber:             1,
		batchMutex:              &sync.Mutex{},
		batchSize:               uint64(options.LazyDiskBatchSize),
		activeDBSessions:        &sync.WaitGroup{},
		bucketPrioritizeLockMap: hashmap.New[int64, *sync.Mutex](),
		buckets:                 hashmap.New[int64, *CorePriorityQueue[d]](),
		nonEmptyBuckets:         NewBucketPriorityQueue(),
	}

	for i := 0; i < options.NumberOfBuckets; i++ {
		pq := NewCorePriorityQueue[d](fsm.nonEmptyBuckets)
		fsm.buckets.Set(int64(i), &pq)
		fsm.bucketPrioritizeLockMap.Set(int64(i), &sync.Mutex{})
	}

	go fsm.lazyDiskLoader()
	go fsm.lazyDiskDeleter()

	return fsm, nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *FSM[d]) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:

		// Decode the command with gob
		var c CommandPayload[d]
		var buf bytes.Buffer
		decoder := gob.NewDecoder(&buf)
		buf.Write(log.Data)
		err := decoder.Decode(&c)
		if err != nil {
			return ApplyResponse[d]{Success: false, Err: err}
		}

		// Perform the operation
		switch c.Operation {

		case EnQueue:
			// Get the priority queue
			pq, _ := f.buckets.Get(c.Data.Priority)

			if f.diskCache {

				// Lock and assign a global batch number
				f.batchMutex.Lock()
				bnum := f.batchNumber
				f.batchCounter++
				if f.batchCounter == f.batchSize {
					f.batchCounter = 0
					f.batchNumber++
				}
				f.syncedBatches.Store(f.batchNumber, false)
				f.batchMutex.Unlock()

				// Assign the batch number to the item
				c.Data.BatchNumber = bnum

				// Gob the data
				var buf bytes.Buffer
				encoder := gob.NewEncoder(&buf)
				err := encoder.Encode(c.Data)
				if err != nil {
					return ApplyResponse[d]{Success: false, Err: err}
				}

				// Write to Disk
				if c.LazyOperation {
					f.sendMessagesProcessed++
					// Add the item to the lazy disk queue
					f.lazyDiskSendChan <- schema.LazyMessageQueueItem{
						ID:               c.Data.DiskUUID,
						Data:             buf.Bytes(),
						TransactionBatch: bnum,
					}
				} else {
					// Write to Disk
					err = f.db.Update(func(txn *badger.Txn) error {
						return txn.Set(c.Data.DiskUUID, buf.Bytes())
					})
					if err != nil {
						return ApplyResponse[d]{Success: false, Err: err}
					}
				}
			}

			// Finally, add the item to the priority queue
			pq.EnQueue(c.Data)
			return ApplyResponse[d]{Success: true}

		case DeQueue:
			// Return an error if there are no items in the queue
			if atomic.LoadUint64(&f.nonEmptyBuckets.ObjectsInQueue) == 0 && atomic.LoadInt64(&f.nonEmptyBuckets.ActiveBuckets) == 0 {
				return ApplyResponse[d]{Success: true, Err: errors.New("No items in queue")}
			}

			// Get the first item from the highest priority bucket
			pb, exists := f.nonEmptyBuckets.Peek()
			if !exists {
				return ApplyResponse[d]{Success: true, Err: errors.New("No items in queue bucket")}
			}
			// Get the priority queue
			pq, _ := f.buckets.Get(pb)

			// Dequeue the item
			bnum, diskUUID, priority, item, err := pq.DeQueue()
			if err != nil {
				return ApplyResponse[d]{Success: false, Err: err}
			}

			// Delete the item from the disk
			if f.diskCache {
				if c.LazyOperation {
					f.deleteMessagesProcessed++
					f.lazyDiskDeleteChan <- schema.LazyMessageQueueItem{
						ID:               diskUUID,
						Data:             nil,
						TransactionBatch: bnum,
					}
				} else {
					err = f.db.Update(func(txn *badger.Txn) error {
						return txn.Delete(diskUUID)
					})
					if err != nil {
						return ApplyResponse[d]{Success: false, Err: err}
					}
				}
			}

			response := ApplyResponse[d]{
				Success:  true,
				Err:      nil,
				Data:     item,
				Priority: priority,
			}

			return response

		case Peek:
			// Return an error if there are no items in the queue
			if atomic.LoadUint64(&f.nonEmptyBuckets.ObjectsInQueue) == 0 && atomic.LoadInt64(&f.nonEmptyBuckets.ActiveBuckets) == 0 {
				return ApplyResponse[d]{Success: false, Err: errors.New("No items in queue")}
			}

			// Get the first item from the highest priority bucket
			pb, exists := f.nonEmptyBuckets.Peek()
			if !exists {
				return ApplyResponse[d]{Success: false, Err: errors.New("No items in queue bucket")}
			}
			// Get the priority queue
			pq, _ := f.buckets.Get(pb)

			// Dequeue the item
			item, err := pq.Peek()
			if err != nil {
				return ApplyResponse[d]{Success: false, Err: err}
			}

			return ApplyResponse[d]{Success: true, Data: item}

		default:
			return ApplyResponse[d]{Success: false, Err: errors.New("Invalid command")}
		}

	}
	return ApplyResponse[d]{Success: false, Err: errors.New("Invalid log type")}
}

func (f *FSM[d]) Restore(rc io.ReadCloser) error {

	// TODO: Implement restore

	return nil
}

func (f *FSM[d]) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (f *FSM[d]) lazyDiskLoader() {
	f.activeDBSessions.Add(1)
	defer f.activeDBSessions.Done()

	f.allBatchesSynced.Add(1)
	defer f.allBatchesSynced.Done()

	batch := make(map[uint64][]schema.LazyMessageQueueItem, 0)
	ticker := time.NewTicker(250 * time.Millisecond)

	for {
		select {
		case item, ok := <-f.lazyDiskSendChan:
			if !ok {
				// Channel is closed, process remaining batch and return
				for k, v := range batch {
					f.processBatch(v)
					batch[k] = batch[k][:0]
					f.syncedBatches.Store(k, true)
				}
				return
			}

			batch[item.TransactionBatch] = append(batch[item.TransactionBatch], item)

			if len(batch[item.TransactionBatch]) >= int(f.batchSize) {
				synced, ok := f.syncedBatches.Load(item.TransactionBatch)
				if ok && synced.(bool) == true {
					f.processBatch(batch[item.TransactionBatch])
					batch[item.TransactionBatch] = batch[item.TransactionBatch][:0]
					f.syncedBatches.Store(item.TransactionBatch, true)
				}
			}

		case <-ticker.C:
			for k, v := range batch {
				if len(v) >= int(f.batchSize) {
					f.processBatch(v)
					batch[k] = batch[k][:0]
					f.syncedBatches.Store(k, true)
				}
			}
		}
	}
}

func (f *FSM[d]) lazyDiskDeleter() {
	f.activeDBSessions.Add(1)
	defer f.activeDBSessions.Done()

	batch := make(map[uint64][]schema.LazyMessageQueueItem, 0)

	for {
		select {
		case item, ok := <-f.lazyDiskDeleteChan:
			if !ok {
				f.allBatchesSynced.Wait()
				// Channel is closed, process remaining batch and return
				for _, v := range batch {
					f.deleteBatch(v)
					batch[item.TransactionBatch] = batch[item.TransactionBatch][:0]
					f.syncedBatches.Delete(item.TransactionBatch)
				}
				return
			}

			// Add the item to the batch
			batch[item.TransactionBatch] = append(batch[item.TransactionBatch], item)

			// If the batch is full, process it and delete the items from the disk cache
			if len(batch[item.TransactionBatch]) >= int(f.batchSize) {
				synced, ok := f.syncedBatches.Load(item.TransactionBatch)
				if ok && synced.(bool) == true {
					f.deleteBatch(batch[item.TransactionBatch])
					batch[item.TransactionBatch] = batch[item.TransactionBatch][:0]
					//f.syncedBatches.Delete(item.TransactionBatch)
				}
			}
		}
	}
}

func (f *FSM[d]) processBatch(batch []schema.LazyMessageQueueItem) error {
	txn := f.db.NewTransaction(true) // Read-write transaction
	defer txn.Discard()

	f.sendBatchesProcessed++

	for i := 0; i < len(batch); i++ {
		f.txnSetProcessed++
		entry := batch[i]
		err := txn.Set(entry.ID, entry.Data)
		if err == badger.ErrTxnTooBig {
			// Commit the transaction and start a new one
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = f.db.NewTransaction(true)
			txn.Set(entry.ID, entry.Data)
		} else if err != nil {
			return err
		}
	}

	// Commit the final transaction, if it has any pending writes
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (f *FSM[d]) deleteBatch(batch []schema.LazyMessageQueueItem) error {
	txn := f.db.NewTransaction(true) // Read-write transaction
	defer txn.Discard()

	f.deleteBatchesProcessed++

	for i := 0; i < len(batch); i++ {
		f.txnDeleteProcessed++
		entry := batch[i]
		err := txn.Delete(entry.ID)
		if err == badger.ErrTxnTooBig {
			// Commit the transaction and start a new one
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = f.db.NewTransaction(true)
			txn.Delete(entry.ID)
		} else if err != nil {
			return err
		}
	}

	// Commit the final transaction, if it has any pending writes
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (f *FSM[d]) Close() {
	close(f.lazyDiskSendChan)
	close(f.lazyDiskDeleteChan)

	// Wait for all db sessions to sync to disk
	f.activeDBSessions.Wait()

	// Sync the disk cache
	f.db.Sync()
	f.db.Close()
}

func (f *FSM[d]) ObjectsInQueue() uint64 {
	return atomic.LoadUint64(&f.nonEmptyBuckets.ObjectsInQueue)
}
