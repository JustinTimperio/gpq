package gpq_test

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq"
)

type TestStruct struct {
	ID   int
	Name string
}

func TestGPQ(t *testing.T) {

	var (
		total    int  = 100000
		print    bool = false
		sent     uint64
		received uint64
	)

	queue := gpq.NewGPQ[TestStruct](10)
	wg := &sync.WaitGroup{}
	wg.Add(17)

	timer := time.Now()
	for i := 0; i < 16; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < total/16; i++ {
				r := rand.Int()
				p := rand.Intn(10)
				timer := time.Now()
				err := queue.EnQueue(TestStruct{
					ID:   r,
					Name: "Test-" + fmt.Sprintf("%d", r)},
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

	time.Sleep(10 * time.Second)

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
