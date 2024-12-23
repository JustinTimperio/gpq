package gpq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/JustinTimperio/gpq/disk"
	"github.com/JustinTimperio/gpq/ftime"
	"github.com/JustinTimperio/gpq/queues"
	"github.com/JustinTimperio/gpq/schema"

	"github.com/google/uuid"
)

// GPQ is a generic priority queue that supports priority levels and timeouts
// It is implemented using a heap for each priority level and a priority queue of non-empty buckets
// It also supports disk caching using badgerDB with the option to lazily disk writes and deletes
// The GPQ is thread-safe and supports concurrent access
type GPQ[d any] struct {
	// options is a struct that contains the options for the GPQ
	options schema.GPQOptions

	// buckets is a map of priority buckets
	queue queues.CorePriorityQueue[d]

	// diskCache is a badgerDB used to store items in the GPQ
	diskCache *disk.Disk[d]
	// activeDBSessions is a wait group for active disk cache sessions
	activeDBSessions *sync.WaitGroup

	// lazyDiskMessageChan is a channel used to send messages to the lazy disk cache
	lazyDiskSendChan chan schema.Item[d]
	// lazyDiskDeleteChan is a channel used to send messages to the lazy disk cache
	lazyDiskDeleteChan chan schema.DeleteMessage
	// batchHandler allows for synchronization of disk cache batches
	batchHandler *BatchHandler[d]
	// batchCounter is used to keep track the current batch number
	batchCounter *BatchCounter
}

// NewGPQ creates a new GPQ with the given number of buckets
// The number of buckets is the number of priority levels you want to support
// You must provide the number of buckets ahead of time and all priorities you submit
// must be within the range of 0 to NumOfBuckets
func NewGPQ[d any](Options schema.GPQOptions) (uint, *GPQ[d], error) {

	var diskCache *disk.Disk[d]
	var err error
	var sender chan schema.Item[d]
	var receiver chan schema.DeleteMessage

	if Options.DiskCacheEnabled {
		diskCache, err = disk.NewDiskCache[d](nil, Options)
		if err != nil {
			return 0, nil, err
		}

		if Options.LazyDiskCacheEnabled {
			sender = make(chan schema.Item[d], Options.LazyDiskCacheChannelSize)
			receiver = make(chan schema.DeleteMessage, Options.LazyDiskCacheChannelSize)
		}
	}

	gpq := &GPQ[d]{
		queue:   queues.NewCorePriorityQueue[d](Options, diskCache, receiver),
		options: Options,

		diskCache:        diskCache,
		activeDBSessions: &sync.WaitGroup{},

		lazyDiskSendChan:   sender,
		lazyDiskDeleteChan: receiver,
		batchHandler:       NewBatchHandler(diskCache),
		batchCounter:       NewBatchCounter(Options.LazyDiskBatchSize),
	}

	var restored uint
	if Options.DiskCacheEnabled {
		items, err := gpq.diskCache.RestoreFromDisk()
		if err != nil {
			return 0, gpq, err
		}

		errs := gpq.restoreDB(items)
		if errs != nil {
			return 0, gpq, fmt.Errorf("Failed to Restore DB, received %d errors! Errors: %v", len(errs), errs)
		}
		restored = uint(len(items))

		if Options.LazyDiskCacheEnabled {
			go gpq.lazyDiskWriter(Options.DiskWriteDelay)
			go gpq.lazyDiskDeleter()
		}
	}

	return restored, gpq, nil
}

// ItemsInQueue returns the total number of items in the queue
func (g *GPQ[d]) ItemsInQueue() uint {
	return g.queue.ItemsInQueue()
}

// ItemsInDB returns the total number of items currently commit to disk
func (g *GPQ[d]) ItemsInDB() uint {
	return g.diskCache.ItemsInDB()
}

// ActiveBuckets returns the total number of buckets(priorities) that have messages within
func (g *GPQ[d]) ActiveBuckets() uint {
	return g.queue.ActiveBuckets()
}

// Enqueue adds an item to the queue with the given options
func (g *GPQ[d]) Enqueue(item schema.Item[d]) error {

	if item.Priority > uint(g.options.MaxPriority) {
		return errors.New("Priority bucket does not exist")
	}
	item.SubmittedAt = ftime.Now()
	item.LastEscalated = item.SubmittedAt

	if g.options.DiskCacheEnabled && !item.WasRestored {
		key, err := uuid.New().MarshalBinary()
		if err != nil {
			return err
		}
		item.DiskUUID = key

		if g.options.LazyDiskCacheEnabled {
			item.BatchNumber = g.batchCounter.Increment()
			g.lazyDiskSendChan <- item
		} else {
			err = g.diskCache.WriteSingle(key, item)
			if err != nil {
				return err
			}
		}
	}

	return g.queue.Enqueue(&item)
}

// EnqueueBatch takes a slice of items and attempts to enqueue them in their perspective buckets
// If a error is generated, it is attached to a slice of errors. Currently the batch will be commit
// in the partial state, and it is up to the user to parse the errors and resend messages that failed.
// In the future this will most likely change with the addition of transactions.
func (g *GPQ[d]) EnqueueBatch(items []schema.Item[d]) []error {

	var (
		errors         []error
		processedItems []*schema.Item[d]
	)

	for i := 0; i < len(items); i++ {
		if items[i].Priority > uint(g.options.MaxPriority) {
			errors = append(errors, fmt.Errorf("No Bucket exists to place message %d with priority %d", i, items[i].Priority))
			continue
		}

		if g.options.DiskCacheEnabled {
			key, err := uuid.New().MarshalBinary()
			if err != nil {
				errors = append(errors, fmt.Errorf("Unable to generate UUID for message %d with priority %d", i, items[i].Priority))
				continue
			}
			items[i].DiskUUID = key

			if g.options.LazyDiskCacheEnabled {
				items[i].BatchNumber = g.batchCounter.Increment()
				g.lazyDiskSendChan <- items[i]
			} else {
				err = g.diskCache.WriteSingle(items[i].DiskUUID, items[i])
				if err != nil {
					errors = append(errors, fmt.Errorf("Unable to write message %d with priority %d", i, items[i].Priority))
					continue
				}
			}
		}

		processedItems = append(processedItems, &items[i])
	}

	return g.queue.EnqueueBatch(processedItems)
}

// Dequeue removes and returns the item with the highest priority in the queue
func (g *GPQ[d]) Dequeue() (item *schema.Item[d], err error) {
	item, err = g.queue.Dequeue()
	if err != nil {
		return item, err
	}

	if g.options.DiskCacheEnabled {
		if g.options.LazyDiskCacheEnabled {
			dm := schema.DeleteMessage{
				DiskUUID:    item.DiskUUID,
				BatchNumber: item.BatchNumber,
				WasRestored: item.WasRestored,
			}

			g.lazyDiskDeleteChan <- dm

		} else {
			err = g.diskCache.DeleteSingle(item.DiskUUID)
			if err != nil {
				return item, err
			}
		}
	}

	return item, nil

}

// DequeueBatch takes a batch size, and returns a slice ordered by priority up to the batchSize provided
// enough messages are present to fill the batch. Partial batches will be returned if a error is encountered.
func (g *GPQ[d]) DequeueBatch(batchSize uint) (items []*schema.Item[d], errs []error) {
	items, errs = g.queue.DequeueBatch(batchSize)
	if errs != nil {
		return items, errs
	}

	if g.options.DiskCacheEnabled {
		for i := 0; i < len(items); i++ {
			if g.options.LazyDiskCacheEnabled {
				dm := schema.DeleteMessage{
					DiskUUID:    items[i].DiskUUID,
					BatchNumber: items[i].BatchNumber,
					WasRestored: items[i].WasRestored,
				}

				g.lazyDiskDeleteChan <- dm

			} else {
				err := g.diskCache.DeleteSingle(items[i].DiskUUID)
				if err != nil {
					return nil, []error{err}
				}
			}
		}
	}

	return items, nil
}

// Prioritize orders the queue based on the individual options added to
// every message in the queue. Prioritizing the queue is a stop-the-world
// event, so consider your usage carefully.
func (g *GPQ[d]) Prioritize() (escalated, removed uint, err error) {
	return g.queue.Prioritize()
}

// Close performs a safe shutdown of the GPQ and the disk cache preventing data loss
func (g *GPQ[d]) Close() {

	if g.options.DiskCacheEnabled {
		if g.options.LazyDiskCacheEnabled {
			close(g.lazyDiskSendChan)
			close(g.lazyDiskDeleteChan)
		}

		// Wait for all db sessions to sync to disk
		g.activeDBSessions.Wait()

		// Safely close the diskCache
		g.diskCache.Close()
	}

}

func (g *GPQ[d]) restoreDB(items []*schema.Item[d]) []error {
	// Quick sanity check
	for i := 0; i < len(items); i++ {
		if items[i].Priority > uint(g.options.MaxPriority) {
			return []error{fmt.Errorf("You are trying to restore items with priorities higher than the max allowed for this queue")}
		}
	}

	return g.queue.EnqueueBatch(items)
}

func (g *GPQ[d]) lazyDiskWriter(maxDelay time.Duration) {
	g.activeDBSessions.Add(1)
	defer g.activeDBSessions.Done()

	var mux sync.Mutex
	var wg sync.WaitGroup
	var closer = make(chan struct{}, 1)
	batch := make(map[uint][]*schema.Item[d], 0)
	ticker := time.NewTicker(maxDelay)

	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case item, ok := <-g.lazyDiskSendChan:
				if !ok {
					closer <- struct{}{}
					mux.Lock()
					for k, v := range batch {
						g.batchHandler.processBatch(v, k)
						batch[k] = batch[k][:0]
					}
					mux.Unlock()
					return
				}

				mux.Lock()
				batch[item.BatchNumber] = append(batch[item.BatchNumber], &item)
				mux.Unlock()
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ticker.C:
				mux.Lock()
				for k, v := range batch {
					if len(v) >= int(g.options.LazyDiskBatchSize) {
						g.batchHandler.processBatch(v, k)
						batch[k] = batch[k][:0]
					}
				}
				mux.Unlock()
			case <-closer:
				return
			}
		}
	}()

	wg.Wait()
}

func (g *GPQ[d]) lazyDiskDeleter() {
	g.activeDBSessions.Add(1)
	defer g.activeDBSessions.Done()

	batch := make(map[uint][]*schema.DeleteMessage, 0)
	restored := make([]*schema.DeleteMessage, 0)

	for {
		select {
		case item, ok := <-g.lazyDiskDeleteChan:
			if !ok {
				g.batchHandler.deleteBatch(restored, 0, true)

				for i, v := range batch {
					g.batchHandler.deleteBatch(v, i, false)
					batch[item.BatchNumber] = batch[item.BatchNumber][:0]
				}
				return
			}

			if item.WasRestored {
				restored = append(restored, &item)
				if len(restored) >= int(g.options.LazyDiskBatchSize) {
					g.batchHandler.deleteBatch(restored, 0, true)
					restored = restored[:0]
				}
				continue
			}

			// If the batch is full, process it and delete the items from the disk cache
			batch[item.BatchNumber] = append(batch[item.BatchNumber], &item)
			if len(batch[item.BatchNumber]) >= int(g.options.LazyDiskBatchSize) {
				g.batchHandler.deleteBatch(batch[item.BatchNumber], item.BatchNumber, false)
				batch[item.BatchNumber] = batch[item.BatchNumber][:0]
			}
		}
	}
}
