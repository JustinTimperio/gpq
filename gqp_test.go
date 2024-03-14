package gpq_test

import (
	"log"
	"math/rand"
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
		total        int  = 1000000
		print        bool = false
		sent         uint64
		timedOut     uint64
		received     uint64
		missed       int64
		hits         int64
		lastPriority int64
	)

	options := schema.GPQOptions{
		NumberOfBuckets:   10,
		DiskCache:         false,
		DiskCachePath:     "/tmp/gpq/topic/cache",
		Compression:       true,
		LazyDiskBatchSize: 1000,

		RaftPoolConnections: 10,
		RaftCacheSize:       1000,
		RaftStableStorePath: "/tmp/gpq/topic/raft/stable",
		RaftSnapshotPath:    "/tmp/gpq/topic/raft/snapshot",
		RaftSnapshotRetain:  5,
		RaftPort:            11000,
		RaftBindAddress:     "127.0.0.1",
		RaftNodeID:          "Test-Node-1",
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

	queue, err := gpq.NewGPQ[int](options)
	if err != nil {
		log.Fatalln(err)
	}
	wg := &sync.WaitGroup{}
	// Wait to become the leader
	time.Sleep(5 * time.Second)

	go func() {
		for queue.FSM.ObjectsInQueue() > 0 || atomic.LoadUint64(&received) < uint64(total) {
			log.Println("Objects in queue", queue.FSM.ObjectsInQueue(), "Received", atomic.LoadUint64(&received), "Timed Out", atomic.LoadUint64(&timedOut))
			time.Sleep(2 * time.Second)
			/*
				time.Sleep(500 * time.Millisecond)
				to, es, err := queue.Prioritize()
				if err != nil {
				}
				atomic.AddUint64(&timedOut, to)
				log.Println("Prioritize Timed Out:", to, "Escalated:", es)
			*/
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
					rand.Int(),
					int64(p),
					true,
					time.Duration(time.Second),
					true,
					time.Duration(time.Second*10),
					true,
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

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for queue.FSM.ObjectsInQueue() > 0 || atomic.LoadUint64(&received)+atomic.LoadUint64(&timedOut) < uint64(total) {
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
					atomic.AddInt64(&missed, 1)
				} else {
					atomic.AddInt64(&hits, 1)
				}
				atomic.StoreInt64(&lastPriority, priority)
			}
			time.Sleep(500 * time.Millisecond)
		}()
	}

	wg.Wait()
	log.Println("Sent", atomic.LoadUint64(&sent), "Received", atomic.LoadUint64(&received), "Timed Out", atomic.LoadUint64(&timedOut), "Finished in", time.Since(timer), "Missed", missed, "Hits", hits)

	// Wait for all db sessions to sync to disk
	queue.Close()

}

func TestNumberOfItems(t *testing.T) {
	var total int
	opts := badger.DefaultOptions("/tmp/gpq/topic/cache")
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
			// Get item
			item := it.Item()
			item.Value(func(val []byte) error {
				/*
					// Decode the item
					var buf bytes.Buffer
					buf.Write(val)
					obj := schema.Item[int]{}
					err = gob.NewDecoder(&buf).Decode(&obj)
					if err != nil {
						return errors.New("Error decoding item from disk cache: " + err.Error())
					}
					json, err := json.Marshal(obj)
					if err != nil {
						return errors.New("Error marshalling item from disk cache: " + err.Error())
					}
					fmt.Println(string(json))
				*/

				return nil
			})

		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Total items in badgerDB", total)
	return
}
