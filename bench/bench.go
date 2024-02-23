package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq"
)

type TestStruct struct {
	ID   int
	Name string
}

// Set the total number of items and if you want to print the results
var (
	total      int  = 20000000
	prioritize bool = true
	print      bool = false
	sent       uint64
	received   uint64
)

func main() {

	// Create a new GPQ with a h-heap width of 100 using the TestStruct as the data type
	queue := gpq.NewGPQ[TestStruct](100)

	// Setup the pprof server if you want to profile
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	// If you want to prioritize the queue, start the prioritize function
	// This will move items to the front of the queue if they have been waiting too long
	if prioritize {
		go func() {
			for {
				count, err := queue.Prioritize()
				time.Sleep(100 * time.Millisecond)

				if err != nil {
					log.Println("Prioritized:", count, "No items to prioritize in", len(err), "buckets")
					continue
				}
				log.Println("Prioritized:", count)
			}
		}()
	}

	// Set up the wait group
	wg := &sync.WaitGroup{}
	timer := time.Now()

	// Launch 4 senders to simulate multiple incoming streams of data
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			sender(queue, total/4)
		}()
	}

	// Launch a receiver to simulate 2 consumers acting asynchronously
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			receiver(queue, total)
		}()
	}

	// Wait for all the senders and receivers to finish
	wg.Wait()

	// Print the results
	log.Println(
		"Sent:", sent,
		"Received:", received,
		"Finished in:", time.Since(timer),
	)

}

func receiver(queue *gpq.GPQ[TestStruct], total int) {
	var lastPriority int64
	for total > int(received) {
		timer := time.Now()
		priority, item, err := queue.DeQueue()
		if err != nil {
			log.Println(err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		atomic.AddUint64(&received, 1)

		if print {
			log.Println("DeQueue", priority, item, time.Since(timer), "Total:", received)
		}
		if lastPriority > priority {
			log.Fatalln("Priority out of order")
		}
	}
}

func sender(queue *gpq.GPQ[TestStruct], total int) {
	for i := 0; i < total; i++ {
		r := rand.Int()
		p := rand.Intn(100)
		timer := time.Now()
		err := queue.EnQueue(TestStruct{
			ID:   r,
			Name: "Test-" + fmt.Sprintf("%d", r)},
			int64(p),
			time.Second,
		)
		if err != nil {
			log.Fatalln(err)
		}
		if print {
			log.Println("EnQueue", p, time.Since(timer), "Total:", sent)
		}
		atomic.AddUint64(&sent, 1)
	}
}
