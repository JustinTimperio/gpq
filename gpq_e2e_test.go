package gpq_test

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
)

func TestE2E(t *testing.T) {
	var (
		total      uint64 = 10_000_000
		syncToDisk bool   = true
		lazySync   bool   = true
		maxBuckets uint   = 10
		batchSize  uint   = 10_000
		senders    uint   = 4
		receivers  uint   = 4
	)

	defaultMessageOptions := schema.EnQueueOptions{
		ShouldEscalate: true,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     true,
		Timeout:        time.Duration(time.Second * 5),
	}

	opts := schema.GPQOptions{
		MaxPriority: maxBuckets,

		DiskCacheEnabled:         syncToDisk,
		DiskCachePath:            "/tmp/gpq/batch-e2e-parallel",
		DiskCacheCompression:     true,
		DiskEncryptionEnabled:    true,
		DiskEncryptionKey:        []byte("12345678901234567890123456789012"),
		LazyDiskCacheChannelSize: 1_000_000,

		DiskWriteDelay:       time.Duration(time.Second * 5),
		LazyDiskCacheEnabled: lazySync,
		LazyDiskBatchSize:    10_000,
	}

	_, queue, err := gpq.NewGPQ[uint](opts)
	if err != nil {
		log.Fatalln(err)
	}

	var (
		received  uint64
		removed   uint
		escalated uint
	)

	var wg sync.WaitGroup
	var shutdown = make(chan struct{}, receivers)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second * 1)

	breaker:
		for {
			select {
			case <-ticker.C:
				r, e, err := queue.Prioritize()
				if err != nil {
					log.Fatalln(err)
				}
				removed += r
				escalated += e
				t.Log("Received:", received, "Removed:", removed, "Escalated:", escalated)

			case <-shutdown:
				break breaker
			}
		}

		t.Log("Exited Prioritize")
	}()

	for i := uint(0); i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := uint(0); j < (uint(total)/batchSize)/senders; j++ {

				var miniBatch []schema.Item[uint]
				for i := uint(0); i < batchSize; i++ {
					p := j % maxBuckets
					item := schema.NewItem(p, j, defaultMessageOptions)
					miniBatch = append(miniBatch, item)
				}

				err := queue.EnqueueBatch(miniBatch)
				if err != nil {
					log.Fatalln(err)
				}
			}
			t.Log("Worker:" + fmt.Sprint(i) + " Enqueued all items")
		}()
	}

	for i := uint(0); i < receivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if atomic.LoadUint64(&received)+uint64(removed) >= total {
					break
				}
				items, err := queue.DequeueBatch(batchSize)
				if err != nil {
					continue
				}
				atomic.AddUint64(&received, uint64(len(items)))
			}
			t.Log("Worker:" + fmt.Sprint(i) + " Dequeued all items")
			shutdown <- struct{}{}
		}()
	}

	wg.Wait()
	if queue.ItemsInQueue() != 0 {
		t.Fatal("Items in queue:", queue.ItemsInQueue())
	}

	t.Log("Waiting for queue to close")
	queue.Close()

	num := numberOfItemsInDB(opts.DiskCachePath)
	if num > 0 {
		log.Fatalln("Items in DB:", num)
	}

	t.Log("Batch Test Passed")
}
