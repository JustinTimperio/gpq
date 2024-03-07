package gpq_test

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/dgraph-io/badger/v4"
)

func TestGPQ(t *testing.T) {

	var (
		total      int  = 100000
		print      bool = false
		syncToDisk bool = true
		lazy       bool = true
		retries    int  = 20
		sent       uint64
		received   uint64
	)

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

	queue, err := gpq.NewGPQ[int](10, syncToDisk, "/tmp/gpq/test", lazy)
	if err != nil {
		log.Fatalln(err)
	}
	wg := &sync.WaitGroup{}

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
					true,
					time.Duration(time.Second),
					true,
					time.Duration(time.Second*10),
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

	var missed int64
	var hits int64

	wg.Add(1)
	for i := 0; i < 1; i++ {
		go func() {
			defer wg.Done()

			var lastPriority int64

			for i := 0; i < retries; i++ {
				for atomic.LoadUint64(&queue.NonEmptyBuckets.ObjectsInQueue) > 0 {
					timer := time.Now()
					priority, item, err := queue.DeQueue()
					if err != nil {
						if print {
							log.Println("Hits", hits, "Misses", missed, "Sent", sent, "Recived", missed+hits, err)
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
				time.Sleep(20 * time.Millisecond)
				log.Println("Retrying", i)
			}
		}()
	}

	wg.Wait()
	log.Println("Sent", atomic.LoadUint64(&sent), "Received", atomic.LoadUint64(&received), "Finished in", time.Since(timer), "Missed", missed, "Hits", hits)

	// Wait for all db sessions to sync to disk
	close(queue.LazyDiskSendChan)
	close(queue.LazyDiskDeleteChan)
	queue.ActiveDBSessions.Wait()
	queue.DiskCache.Sync()
	queue.DiskCache.Close()

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
