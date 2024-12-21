package main

import (
	"encoding/csv"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/JustinTimperio/gpq/v1"
	"github.com/JustinTimperio/gpq/v1/schema"
)

var (
	maxTotal              int = 10000000
	nMaxBuckets           int = 100
	defaultMessageOptions     = schema.EnQueueOptions{
		ShouldEscalate: true,
		EscalationRate: time.Duration(time.Second),
		CanTimeout:     true,
		Timeout:        time.Duration(time.Second * 5),
	}
)

func main() {
	// Setup the pprof server if you want to profile
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	var lazy = false
	var disk = false

	iter(true, disk, lazy)
	iter(false, disk, lazy)

}

func iter(prioritize bool, disk, lazy bool) {

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
	header := []string{"Total Items", "Buckets", "Removed", "Escalated", "Time Elapsed", "Time to Send", "Time to Receive"}
	writer.Write(header)

	// Test the bench function for each million entries up to maxTotal
	for total := 1000000; total <= maxTotal; total += 1000000 {
		// Test the bench function for each increment of 10 buckets
		for buckets := 5; buckets <= nMaxBuckets; buckets += 5 {
			log.Println("Starting test for", total, "entries and", buckets, "buckets")

			// Run the bench function
			totalElapsed, sent, received, removed, escalated := bench(total, buckets, prioritize, disk, lazy)
			runtime.GC()

			// Write the statistics to the CSV file
			stats := []string{
				strconv.Itoa(total),
				strconv.Itoa(buckets),
				strconv.Itoa(int(removed)),
				strconv.Itoa(int(escalated)),
				strconv.FormatFloat(totalElapsed.Seconds(), 'f', 6, 64),
				strconv.FormatFloat(sent.Seconds(), 'f', 6, 64),
				strconv.FormatFloat(received.Seconds(), 'f', 6, 64),
			}
			writer.Write(stats)
		}
	}
}

func bench(total int, buckets int, prioritize bool, disk bool, lazy bool) (totalElapsed time.Duration, sentElapsed time.Duration, receivedElapsed time.Duration, removed uint64, escalated uint64) {

	opts := schema.GPQOptions{
		MaxPriority:              uint(buckets),
		DiskCacheEnabled:         disk,
		DiskCachePath:            "/tmp/gpq/graphs",
		DiskCacheCompression:     false,
		DiskEncryptionEnabled:    false,
		DiskEncryptionKey:        []byte("12345678901234567890123456789012"),
		LazyDiskCacheEnabled:     lazy,
		LazyDiskBatchSize:        10_000,
		LazyDiskCacheChannelSize: uint(total),
		DiskWriteDelay:           time.Duration(5 * time.Second),
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
				time.Sleep(1 * time.Second)
				timedOut, prioritized, err := queue.Prioritize()
				if err != nil {
					log.Fatalln(err)
				}
				escalated += uint64(prioritized)
				removed += uint64(timedOut)
			}
		}()
	}

	timer := time.Now()
	for i := 0; i < total; i++ {
		p := i % buckets
		item := schema.NewItem(uint(p), i, defaultMessageOptions)
		err := queue.Enqueue(item)
		if err != nil {
			log.Fatalln(err)
		}
	}
	sendTime := time.Since(timer)

	timer = time.Now()
	breaker := 0
	for i := 0; i < total; i++ {
		_, err := queue.Dequeue()
		if err != nil {
			if breaker > 1000 {
				log.Println("Length of queue:", queue.ItemsInQueue())
				log.Panicln("An error occurred while dequeuing:", err, "at index", i, "of", total, "with total removed", removed)
			}
			if removed+uint64(i) == uint64(total) {
				break
			}
			breaker++
			i--
		}
	}
	receiveTime := time.Since(timer)

	// Wait for all db sessions to sync to disk
	queue.Close()
	return sendTime + receiveTime, sendTime, receiveTime, removed, escalated

}
