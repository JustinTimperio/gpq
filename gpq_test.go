package gpq_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"

	"github.com/dgraph-io/badger/v4"
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

	_, queue, err := gpq.NewGPQ[uint](opts)
	if err != nil {
		log.Fatalln(err)
	}

	var order = make(map[uint][]schema.Item[uint])
	for i := uint(0); i < total; i++ {
		p := i % maxBuckets
		item := schema.NewItem(p, i, defaultMessageOptions)

		err := queue.Enqueue(item)
		if err != nil {
			log.Fatalln(err)
		}

		_, ok := order[p]
		if !ok {
			order[p] = make([]schema.Item[uint], 0)
		}
		order[p] = append(order[p], item)
	}

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
	var (
		total      uint = 1_000_000
		syncToDisk bool = false
		lazySync   bool = false
		maxBuckets uint = 10
	)

	defaultMessageOptions := schema.EnQueueOptions{
		ShouldEscalate: true,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     true,
		Timeout:        time.Duration(time.Second * 10),
	}

	opts := schema.GPQOptions{
		MaxPriority: maxBuckets,

		DiskCacheEnabled:         syncToDisk,
		DiskCachePath:            "/tmp/gpq/test-prioritize",
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
				r, e := queue.Prioritize()
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
			if received+removed >= total {
				break
			}
			time.Sleep(time.Millisecond * 100)
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
	if !(received != 0 && removed != 0 && escalated != 0) {
		t.Fatal("Prioritize failed", received, removed, escalated)
	}
	if queue.ItemsInQueue() != 0 {
		t.Fatal("Items in queue:", queue.ItemsInQueue())
	}

	queue.Close()
	t.Log("Received:", received, "Removed:", removed, "Escalated:", escalated)
}

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
	t.Log("Batch Test Passed")
}

func TestE2EParallel(t *testing.T) {
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
		DiskCacheCompression:     false,
		DiskEncryptionEnabled:    false,
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
				r, e := queue.Prioritize()
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

func numberOfItemsInDB(path string) int {
	var total int
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return 0
	}

	db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {

			val := it.Item()
			val.Value(func(v []byte) error {
				var buf bytes.Buffer
				buf.Write(v)
				obj := schema.Item[int]{}
				err = gob.NewDecoder(&buf).Decode(&obj)
				if err != nil {
					return err
				}
				jsonObj, err := json.MarshalIndent(obj, "", "  ")
				if err != nil {
					return err
				}
				fmt.Println(string(jsonObj))
				total++
				return nil

			})
		}

		return nil
	})

	return total
}
