package gpq_test

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
	"github.com/dgraph-io/badger/v4"
)

func TestGPQ(t *testing.T) {

	var (
		total                 int  = 10000000
		print                 bool = false
		sent                  uint64
		timedOut              uint64
		received              uint64
		missed                int64
		hits                  int64
		defaultMessageOptions = schema.EnQueueOptions{
			ShouldEscalate: true,
			EscalationRate: time.Duration(time.Second),
			CanTimeout:     true,
			Timeout:        time.Duration(time.Second * 10),
		}
	)

	opts := schema.GPQOptions{
		NumberOfBuckets:       10,
		DiskCacheEnabled:      true,
		DiskCachePath:         "/tmp/gpq/test",
		DiskCacheCompression:  false,
		DiskEncryptionEnabled: false,
		DiskEncryptionKey:     []byte("12345678901234567890123456789012"),
		LazyDiskCacheEnabled:  true,
		LazyDiskBatchSize:     1000,
	}

	// Create a pprof file
	f, err := os.Create("profile.pprof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Start CPU profiling
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	// Create pprof mutex file
	fm, err := os.Create("profile.mutex")
	if err != nil {
		log.Fatal(err)
	}
	defer fm.Close()

	// Start mutex profiling
	runtime.SetMutexProfileFraction(1)
	defer func() {
		p := pprof.Lookup("mutex")
		if p == nil {
			log.Fatal("could not capture mutex profile")
		}
		// Create pprof mutex file
		fm, err := os.Create("profile.mutex")
		if err != nil {
			log.Fatal(err)
		}
		defer fm.Close()
		if err := p.WriteTo(fm, 0); err != nil {
			log.Fatal("could not write mutex profile: ", err)
		}
	}()

	requeued, queue, err := gpq.NewGPQ[int](opts)
	if err != nil {
		log.Fatalln(err)
	}
	wg := &sync.WaitGroup{}

	go func() {
		for atomic.LoadUint64(&queue.NonEmptyBuckets.ObjectsInQueue) > 0 || atomic.LoadUint64(&received) < uint64(total) {
			time.Sleep(500 * time.Millisecond)
			to, es, err := queue.Prioritize()
			if err != nil {
			}
			atomic.AddUint64(&timedOut, to)
			log.Println("Prioritize Timed Out:", to, "Escalated:", es)
		}
	}()

	timer := time.Now()
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < total/10; i++ {
				p := i % 10
				timer := time.Now()
				err := queue.EnQueue(
					i,
					int64(p),
					defaultMessageOptions,
				)
				if err != nil {
					log.Fatalln(err)
				}
				if print {
					log.Println("EnQueue", p, time.Since(timer))
				}
				atomic.AddUint64(&sent, 1)
			}
		}()
	}

	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			var lastPriority int64
			for retries := 0; retries < 50; retries++ {
				for atomic.LoadUint64(&queue.NonEmptyBuckets.ObjectsInQueue) > 0 || atomic.LoadUint64(&received)+atomic.LoadUint64(&timedOut) < uint64(total) {
					timer := time.Now()
					priority, item, err := queue.DeQueue()
					if err != nil {
						if print {
							log.Println("Hits", hits, "Misses", missed, "Sent", sent, "Received", missed+hits, err)
						}
						time.Sleep(10 * time.Millisecond)
						lastPriority = 0
						continue
					}
					atomic.AddUint64(&received, 1)
					if print {
						log.Println("DeQueue", priority, received, item, time.Since(timer))
					}

					if lastPriority > priority {
						missed++
					} else {
						hits++
					}
					lastPriority = priority
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	fmt.Println(
		" Total:", total, "\n",
		"Sent:", atomic.LoadUint64(&sent), "\n",
		"Received:", atomic.LoadUint64(&received), "\n",
		"Requeued:", requeued, "\n",
		"Timed Out:", atomic.LoadUint64(&timedOut), "\n",
		"Finished in:", time.Since(timer), "\n",
		"Out Of Order:", missed, "\n",
		"In Order:", hits,
	)

	// Wait for all db sessions to sync to disk
	queue.Close()

}

func TestNumberOfItems(t *testing.T) {
	var total int
	opts := badger.DefaultOptions("/tmp/gpq/test")
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			val := it.Item()
			val.Value(func(v []byte) error {

				/*
					// Decode the item
					var buf bytes.Buffer
					buf.Write(v)
					obj := schema.Item[int]{}
					err = gob.NewDecoder(&buf).Decode(&obj)
					if err != nil {
						t.Fatal(err)
					}
					jsonObj, err := json.Marshal(obj)
					if err != nil {
						t.Fatal(err)
					}
					fmt.Println(string(jsonObj))
				*/

				return nil
			})

			total++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Total items in badgerDB", total)
	return
}
