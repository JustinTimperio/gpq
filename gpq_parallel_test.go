package gpq_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq/v1"
	"github.com/JustinTimperio/gpq/v1/schema"
)

// Tests pushing and pulling single messages in parallel
func TestSingleParallel(t *testing.T) {
	var (
		total      uint = 10_000_000
		syncToDisk bool = false
		lazySync   bool = false
		maxBuckets uint = 10
	)

	defaultMessageOptions := schema.EnQueueOptions{
		ShouldEscalate: false,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     false,
		Timeout:        time.Duration(time.Second * 10),
	}

	opts := schema.GPQOptions{
		MaxPriority: maxBuckets,

		DiskCacheEnabled:         syncToDisk,
		DiskCachePath:            "/tmp/gpq/batch-parallel",
		DiskCacheCompression:     false,
		DiskEncryptionEnabled:    false,
		DiskEncryptionKey:        []byte("12345678901234567890123456789012"),
		LazyDiskCacheChannelSize: 1_000_000,

		DiskWriteDelay:       time.Duration(time.Second),
		LazyDiskCacheEnabled: lazySync,
		LazyDiskBatchSize:    10_000,
	}

	_, queue, err := gpq.NewGPQ[uint](opts)
	if err != nil {
		log.Fatalln(err)
	}

	var (
		received uint
	)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := uint(0); j < total; j++ {
			p := j % maxBuckets
			item := schema.NewItem(p, j, defaultMessageOptions)

			err := queue.Enqueue(item)
			if err != nil {
				log.Fatalln(err)
			}
		}
		t.Log("Enqueued all items")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if received >= total {
				break
			}
			_, err := queue.Dequeue()
			if err != nil {
				continue
			}
			received++
		}
		t.Log("Dequeued all items")
	}()

	wg.Wait()
	if queue.ItemsInQueue() != 0 {
		t.Fatal("Items in queue:", queue.ItemsInQueue())
	}

	queue.Close()
	t.Log("Single Parallel Test Passed")
}

// Tests pushing and pulling batches of messages in parallel
func TestBatchParallel(t *testing.T) {
	var (
		total      uint = 10_000_000
		syncToDisk bool = false
		lazySync   bool = false
		maxBuckets uint = 10
		batchSize  uint = 10_000
	)

	defaultMessageOptions := schema.EnQueueOptions{
		ShouldEscalate: false,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     false,
		Timeout:        time.Duration(time.Second * 10),
	}

	opts := schema.GPQOptions{
		MaxPriority: maxBuckets,

		DiskCacheEnabled:         syncToDisk,
		DiskCachePath:            "/tmp/gpq/batch-parallel",
		DiskCacheCompression:     false,
		DiskEncryptionEnabled:    false,
		DiskEncryptionKey:        []byte("12345678901234567890123456789012"),
		LazyDiskCacheChannelSize: 1_000_000,

		DiskWriteDelay:       time.Duration(time.Second),
		LazyDiskCacheEnabled: lazySync,
		LazyDiskBatchSize:    10_000,
	}

	_, queue, err := gpq.NewGPQ[uint](opts)
	if err != nil {
		log.Fatalln(err)
	}

	var (
		received uint
	)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := uint(0); j < total/batchSize; j++ {

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
		t.Log("Enqueued all items")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if received >= total {
				break
			}
			items, err := queue.DequeueBatch(batchSize)
			if err != nil {
				continue
			}
			received += uint(len(items))
		}
		t.Log("Dequeued all items")
	}()

	wg.Wait()
	if queue.ItemsInQueue() != 0 {
		t.Fatal("Items in queue:", queue.ItemsInQueue())
	}

	queue.Close()
	t.Log("Batch Parallel Test Passed")
}
