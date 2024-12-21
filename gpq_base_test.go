package gpq_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq/v1"
	"github.com/JustinTimperio/gpq/v1/schema"
)

func TestOrder(t *testing.T) {
	var (
		total      uint = 1_000_000
		syncToDisk bool = false
		lazySync   bool = false
		maxBuckets uint = 10
	)

	defaultMessageOptions := schema.EnQueueOptions{
		ShouldEscalate: false,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     false,
		Timeout:        time.Duration(time.Second * 1),
	}

	opts := schema.GPQOptions{
		MaxPriority: maxBuckets,

		DiskCacheEnabled:         syncToDisk,
		DiskCachePath:            "/tmp/gpq/test-order",
		DiskCacheCompression:     false,
		DiskEncryptionEnabled:    false,
		DiskEncryptionKey:        []byte("1234567890"),
		LazyDiskCacheChannelSize: 1_000_000,

		DiskWriteDelay:       time.Duration(time.Second),
		LazyDiskCacheEnabled: lazySync,
		LazyDiskBatchSize:    10_000,
	}

	_, queue, err := gpq.NewGPQ[string](opts)
	if err != nil {
		log.Fatalln(err)
	}

	var order = make(map[uint][]schema.Item[string])

	// Add the messages to the queue in order
	for i := uint(0); i < total; i++ {
		p := i % maxBuckets
		item := schema.NewItem(p, randomString(30), defaultMessageOptions)

		err := queue.Enqueue(item)
		if err != nil {
			log.Fatalln(err)
		}

		_, ok := order[p]
		if !ok {
			order[p] = make([]schema.Item[string], 0)
		}
		order[p] = append(order[p], item)
	}

	// Pull off the queue and verify order
	for i := uint(0); i < total; i++ {
		item, err := queue.Dequeue()
		if err != nil {
			log.Fatalln(err)
		}

		orderMap := order[item.Priority]
		if orderMap[0].Data != item.Data {
			log.Fatalln("Order mismatch", orderMap[0], item)
		}
		order[item.Priority] = orderMap[1:]
	}

	queue.Close()

}

func TestPrioritize(t *testing.T) {

	defaultMessageOptions := schema.EnQueueOptions{
		ShouldEscalate: true,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     true,
		Timeout:        time.Duration(time.Second * 10),
	}

	ptest := func(tm uint, sd bool, ls bool, mb uint) {
		opts := schema.GPQOptions{
			MaxPriority: mb,

			DiskCacheEnabled:         sd,
			DiskCachePath:            "/tmp/gpq/test-prioritize",
			DiskCacheCompression:     false,
			DiskEncryptionEnabled:    false,
			DiskEncryptionKey:        []byte("12345678901234567890123456789012"),
			LazyDiskCacheChannelSize: tm / 2,

			DiskWriteDelay:       time.Duration(time.Second),
			LazyDiskCacheEnabled: ls,
			LazyDiskBatchSize:    10_000,
		}

		_, queue, err := gpq.NewGPQ[uint](opts)
		if err != nil {
			log.Fatalln(err)
		}

		var (
			escalated uint
			removed   uint
			received  uint
		)

		var wg sync.WaitGroup
		shutdown := make(chan struct{})

		wg.Add(1)
		go func() {
			defer wg.Done()

			ticker := time.NewTicker(time.Second * 1)

		forloop:
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
					break forloop
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := uint(0); j < tm; j++ {
				p := j % mb
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
				if received+removed >= tm {
					break
				}
				time.Sleep(time.Millisecond * 10)
				_, err := queue.Dequeue()
				if err != nil {
					continue
				}
				received++
			}
			t.Log("Dequeued all items")
			shutdown <- struct{}{}
		}()

		wg.Wait()

		if received == 0 || removed == 0 || escalated == 0 {
			t.Fatal("Prioritize failed, no value should be zero: received -", received, "removed -", removed, "escalated -", escalated)
		}

		if queue.ItemsInQueue() != 0 {
			t.Fatal("Items in queue:", queue.ItemsInQueue())
		}

		queue.Close()
		t.Log("Received:", received, "Removed:", removed, "Escalated:", escalated)
	}

	// Test Without Disk Features
	ptest(1_000_000, false, false, 10)
	// Test With Disk Features
	ptest(1_000_000, true, true, 10)
}

func TestRestoreOrder(t *testing.T) {
	var (
		total      uint = 250_000
		syncToDisk bool = true
		lazySync   bool = true
		maxBuckets uint = 10
	)

	defaultMessageOptions := schema.EnQueueOptions{
		ShouldEscalate: false,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     false,
		Timeout:        time.Duration(time.Second * 1),
	}

	opts := schema.GPQOptions{
		MaxPriority: maxBuckets,

		DiskCacheEnabled:         syncToDisk,
		DiskCachePath:            "/tmp/gpq/test-restore",
		DiskCacheCompression:     false,
		DiskEncryptionEnabled:    false,
		DiskEncryptionKey:        []byte("1234567890"),
		LazyDiskCacheChannelSize: 1_000_000,

		DiskWriteDelay:       time.Duration(time.Second),
		LazyDiskCacheEnabled: lazySync,
		LazyDiskBatchSize:    10_000,
	}

	_, queue, err := gpq.NewGPQ[string](opts)
	if err != nil {
		log.Fatalln(err)
	}

	var order = make(map[uint][]schema.Item[string])

	// Add the messages to the queue in order
	for i := uint(0); i < total; i++ {
		p := i % maxBuckets
		item := schema.NewItem(p, randomString(30), defaultMessageOptions)

		err := queue.Enqueue(item)
		if err != nil {
			log.Fatalln(err)
		}

		_, ok := order[p]
		if !ok {
			order[p] = make([]schema.Item[string], 0)
		}
		order[p] = append(order[p], item)
	}

	// Close the queue
	queue.Close()

	// Rebuild the queue
	restored, queue, err := gpq.NewGPQ[string](opts)
	if err != nil {
		log.Fatalln(err)
	}

	if restored == 0 {
		log.Fatalln("No items were restored")
	}

	var lastHigh = uint(0)

	// Pull off the queue and verify order
	for i := uint(0); i < total; i++ {
		item, err := queue.Dequeue()
		if err != nil {
			log.Fatalln(err)
		}

		if item.Priority < lastHigh {
			log.Fatalln("Priority order mismatch", lastHigh, maxBuckets)
		}
		lastHigh = item.Priority

		orderMap := order[item.Priority]
		var found bool
		for _, m := range orderMap {
			if m.Data == item.Data {
				found = true
				break
			}
		}
		if !found {
			log.Fatalln("Message not found in correct bucket")
		}
	}

	log.Println("Restore Order test passed")

}
