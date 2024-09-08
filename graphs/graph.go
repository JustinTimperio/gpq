package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
)

var (
	maxTotal              int  = 10000000
	print                 bool = false
	retries               int  = 5
	nMaxBuckets           int  = 100
	lazy                  bool = true
	defaultMessageOptions      = schema.EnQueueOptions{
		ShouldEscalate: true,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     true,
		Timeout:        time.Duration(time.Second * 10),
	}
)

func main() {

	// Setup the pprof server if you want to profile
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	iter(true)
	iter(false)

}

func iter(prioritize bool) {

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
		for buckets := 5; buckets <= nMaxBuckets; buckets += 5 {
			log.Println("Starting test for", total, "entries and", buckets, "buckets")

			// Reset the sent, received, and reprioritized counters
			var sent uint64
			var received uint64
			var reprioritized uint64

			// Run the bench function
			t := bench(total, prioritize, print, buckets, &sent, &received, &reprioritized, lazy)

			// Write the statistics to the CSV file
			stats := []string{
				strconv.Itoa(total),
				strconv.Itoa(buckets),
				strconv.FormatUint(sent, 10),
				strconv.FormatUint(received, 10),
				t,
				strconv.FormatUint(reprioritized, 10),
			}
			writer.Write(stats)
		}
	}
}

func bench(total int, prioritize bool, print bool, nBuckets int, sent *uint64, received *uint64, reprioritized *uint64, lazy bool) string {

	opts := schema.GPQOptions{
		MaxPriority:           nBuckets,
		DiskCacheEnabled:      true,
		DiskCachePath:         "/tmp/gpq/test",
		DiskCacheCompression:  false,
		DiskEncryptionEnabled: false,
		DiskEncryptionKey:     []byte("12345678901234567890123456789012"),
		LazyDiskCacheEnabled:  lazy,
		LazyDiskBatchSize:     1000,
	}

	// Create a new GPQ with a h-heap width of 100 using the TestStruct as the data type
	_, queue, err := gpq.NewGPQ[int](opts)
	if err != nil {
		log.Fatalln(err)
	}

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

	wg := &sync.WaitGroup{}

	wg.Add(10)
	timer := time.Now()
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < total/10; i++ {
				p := i % nBuckets
				timer := time.Now()
				err := queue.Enqueue(
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
				atomic.AddUint64(sent, 1)
			}
		}()
	}

	var missed int64
	var hits int64

	wg.Add(1)
	go func() {
		defer wg.Done()

		var lastPriority int64

		for i := 0; i < retries; i++ {
			for total > int(*received) {
				timer := time.Now()
				priority, item, err := queue.Dequeue()
				if err != nil {
					lastPriority = 0
					continue
				}
				i = retries

				atomic.AddUint64(received, 1)

				if print {
					log.Println("DeQueue", priority, received, item, time.Since(timer))
				}

				if lastPriority > priority {
					atomic.AddInt64(&missed, 1)
				} else {
					atomic.AddInt64(&hits, 1)
				}
				lastPriority = priority
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	log.Println("Sent", *sent, "Received", *received, "Finished in", time.Since(timer), "Missed", missed, "Hits", hits, "Reprioritized", *reprioritized)

	// Wait for all db sessions to sync to disk
	queue.Close()
	return fmt.Sprintf("%d", time.Since(timer).Milliseconds())

}
