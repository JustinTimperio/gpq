package main

import (
	"log"
	"math/rand"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq"
)

var (
	total      int  = 10000000
	prioritize bool = false
	print      bool = false
	maxBuckets int  = 100
	sent       uint64
	received   uint64
)

func main() {

	queue := gpq.NewGPQ[int](10)
	wg := &sync.WaitGroup{}
	wg.Add(17)

	timer := time.Now()
	for i := 0; i < 16; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < total/16; i++ {
				p := rand.Intn(10)
				timer := time.Now()
				err := queue.EnQueue(
					i,
					int64(p),
					time.Minute,
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

		for total > int(received) {
			timer := time.Now()
			priority, item, err := queue.DeQueue()
			if err != nil {
				log.Println(err)
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
	}()

	wg.Wait()
	log.Println("Sent", sent, "Received", received, "Finished in", time.Since(timer), "Missed", missed, "Hits", hits)

}
