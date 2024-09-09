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
	lazyDiskDeleteChan chan schema.Item[d]
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
	var receiver chan schema.Item[d]

	if Options.DiskCacheEnabled {
		diskCache, err = disk.NewDiskCache[d](nil, Options)
		if err != nil {
			return 0, nil, err
		}

		if Options.LazyDiskCacheEnabled {
			sender = make(chan schema.Item[d], Options.LazyDiskCacheChannelSize)
			receiver = make(chan schema.Item[d], Options.LazyDiskCacheChannelSize)
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

	if Options.LazyDiskCacheEnabled && Options.DiskCacheEnabled {
		go gpq.lazyDiskWriter(Options.DiskWriteDelay)
		go gpq.lazyDiskDeleter()
	}

	var restored uint

	return restored, gpq, nil
}

// Len returns the number of items in the GPQ across all buckets
func (g *GPQ[d]) ItemsInQueue() uint {
	return g.queue.ItemsInQueue()
}

func (g *GPQ[d]) ItemsInDB() uint {
	return g.diskCache.ItemsInDB()
}

func (g *GPQ[d]) ActiveBuckets() uint {
	return g.queue.ActiveBuckets()
}

// Enqueue adds an item to the GPQ with the given priority and options
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

func (g *GPQ[d]) EnqueueBatch(items []schema.Item[d]) error {

	for i := 0; i < len(items); i++ {
		if items[i].Priority > uint(g.options.MaxPriority) {
			return fmt.Errorf("Priority bucket does not exist: %d", items[i].Priority)
		}
	}

	if g.options.DiskCacheEnabled {
		for i := 0; i < len(items); i++ {
			key, err := uuid.New().MarshalBinary()
			if err != nil {
				return err
			}
			items[i].DiskUUID = key
		}

		if g.options.LazyDiskCacheEnabled {
			items[0].BatchNumber = g.batchCounter.Increment()
			for i := 0; i < len(items); i++ {
				g.lazyDiskSendChan <- items[i]
			}
		} else {
			for i := 0; i < len(items); i++ {
				err := g.diskCache.WriteSingle(items[i].DiskUUID, items[i])
				if err != nil {
					return err
				}
			}
		}
	}

	return g.queue.EnqueueBatch(&items)
}

func (g *GPQ[d]) Dequeue() (item schema.Item[d], err error) {
	i, err := g.queue.Dequeue()
	if err != nil {
		return item, err
	}
	item = *i

	if g.options.DiskCacheEnabled {
		if g.options.LazyDiskCacheEnabled {
			g.lazyDiskDeleteChan <- item
		} else {
			err = g.diskCache.DeleteSingle(item.DiskUUID)
			if err != nil {
				return item, err
			}
		}
	}

	return item, nil

}

func (g *GPQ[d]) DequeueBatch(batchSize uint) (items []schema.Item[d], err error) {
	i, err := g.queue.DequeueBatch(batchSize)
	if err != nil {
		return nil, err
	}
	items = *i

	if g.options.DiskCacheEnabled {
		if g.options.LazyDiskCacheEnabled {
			for i := 0; i < len(items); i++ {
				g.lazyDiskDeleteChan <- items[i]
			}
		} else {
			for i := 0; i < len(items); i++ {
				err = g.diskCache.DeleteSingle(items[i].DiskUUID)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return items, nil
}

func (g *GPQ[d]) Prioritize() (escalated, removed uint, err error) {
	return g.queue.Prioritize()
}

// Close performs a safe shutdown of the GPQ and the disk cache preventing data loss
func (g *GPQ[d]) Close() {

	if g.options.LazyDiskCacheEnabled && g.options.DiskCacheEnabled {
		close(g.lazyDiskSendChan)
		close(g.lazyDiskDeleteChan)
	}

	// Wait for all db sessions to sync to disk
	g.activeDBSessions.Wait()

	// Sync the disk cache
	if g.options.DiskCacheEnabled {
		g.diskCache.Close()
	}
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

	batch := make(map[uint][]*schema.Item[d], 0)
	restored := make([]*schema.Item[d], 0)

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
