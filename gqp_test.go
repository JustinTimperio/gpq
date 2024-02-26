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
)

func TestGPQ(t *testing.T) {

	var (
		total      int  = 1000000
		print      bool = false
		syncToDisk bool = true
		retries    int  = 10
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

	queue, err := gpq.NewGPQ[int](10, syncToDisk, "/tmp/gpq/")
	if err != nil {
		log.Fatalln(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(21)

	timer := time.Now()
	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < total/20; i++ {
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

	go func() {
		defer wg.Done()

		var lastPriority int64

		for i := 0; i < retries; i++ {
			for atomic.LoadUint64(&queue.TotalLen) > 0 {
				timer := time.Now()
				priority, item, err := queue.DeQueue()
				if err != nil {
					log.Println(sent, missed+hits, err)
					time.Sleep(10 * time.Millisecond)
					lastPriority = 0
					continue
				}
				received++
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
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()
	log.Println("Sent", sent, "Received", received, "Finished in", time.Since(timer), "Missed", missed, "Hits", hits)

	// Wait for all db sessions to sync to disk
	queue.ActiveDBSessions.Wait()

}
