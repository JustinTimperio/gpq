package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq"
)

type TestStruct struct {
	ID   int
	Name string
}

var (
	maxTotal    int  = 10000000
	prioritize  bool = true
	print       bool = false
	nMaxBuckets int  = 100
)

func main() {

	// Setup the pprof server if you want to profile
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	var name = "bench-report-no-repro.csv"
	if prioritize {
		name = "bench-report-repro.csv"
	}

	// Open the CSV file for writing
	os.Remove(name)
	file, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header row to the CSV file
	header := []string{"Total", "nBuckets", "Sent", "Received", "Duration", "Reprioritized"}
	writer.Write(header)

	// Test the bench function for each million entries up to maxTotal
	for total := 1000000; total <= maxTotal; total += 1000000 {
		// Test the bench function for each increment of 10 buckets
		for buckets := 10; buckets <= nMaxBuckets; buckets += 10 {
			log.Println("Starting test for", total, "entries and", buckets, "buckets")

			// Reset the sent, received, and reprioritized counters
			var sent uint64
			var received uint64
			var reprioritized uint64

			// Run the bench function
			t := bench(total, prioritize, print, buckets, &sent, &received, &reprioritized)

			// Write the statistics to the CSV file
			stats := []string{
				strconv.Itoa(total),
				strconv.Itoa(buckets),
				strconv.FormatUint(sent, 10),
				strconv.FormatUint(received, 10),
				t,
				strconv.FormatUint(reprioritized, 10),
			}
			fmt.Println(stats)
			writer.Write(stats)
		}
	}
}

func bench(total int, prioritize bool, print bool, nBuckets int, sent *uint64, received *uint64, reprioritized *uint64) string {

	// Create a new GPQ with a h-heap width of 100 using the TestStruct as the data type
	queue := gpq.NewGPQ[TestStruct](nBuckets)

	// If you want to prioritize the queue, start the prioritize function
	// This will move items to the front of the queue if they have been waiting too long
	if prioritize {
		go func() {
			for {
				timedOut, prioritized, err := queue.Prioritize()
				time.Sleep(100 * time.Millisecond)
				if print {
					log.Println("Prioritized:", prioritized, "Timeouts", timedOut, "No items to prioritize in", len(err), "buckets")
				}

				if err != nil {
					continue
				}
				atomic.AddUint64(reprioritized, uint64(prioritized))
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
			sender(queue, total/4, sent, nBuckets)
		}()
	}

	// Launch a receiver to simulate 2 consumers acting asynchronously
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			receiver(queue, total, received)
		}()
	}

	// Wait for all the senders and receivers to finish
	wg.Wait()

	// Print the results
	log.Println(
		"Sent:", *sent,
		"Received:", *received,
		"Finished in:", time.Since(timer),
		"Reprioritized:", *reprioritized,
	)

	return fmt.Sprintf("%d", time.Since(timer).Milliseconds())

}

func receiver(queue *gpq.GPQ[TestStruct], total int, received *uint64) {
	var lastPriority int64
	for total > int(*received) {
		timer := time.Now()
		priority, item, err := queue.DeQueue()
		if err != nil {
			if print {
				log.Println(err)
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		atomic.AddUint64(received, 1)

		if print {
			log.Println("DeQueue", priority, item, time.Since(timer), "Total:", received)
		}
		if lastPriority > priority {
			log.Fatalln("Priority out of order")
		}
	}
}

func sender(queue *gpq.GPQ[TestStruct], total int, sent *uint64, buckets int) {
	for i := 0; i < total; i++ {
		r := rand.Int()
		p := rand.Intn(buckets)
		timer := time.Now()
		err := queue.EnQueue(TestStruct{
			ID:   r,
			Name: "Test-" + fmt.Sprintf("%d", r)},
			int64(p),
			false,
			10*time.Millisecond,
			false,
			10*time.Second,
		)
		if err != nil {
			log.Fatalln(err)
		}
		if print {
			log.Println("EnQueue", p, time.Since(timer), "Total:", sent)
		}
		atomic.AddUint64(sent, 1)
	}
}
