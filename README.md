<p align="center">
  <img src="./docs/gpq.png">
</p>

<h4 align="center">
	GPQ is an extremely fast and flexible priority queue, capable of a few million transactions a second when run in RAM and tens of thousands of transactions a second when synced to disk. GPQ supports a complex "Double Priority Queue" which allows for priorities to be distributed across N buckets, with each bucket holding a second priority queue which allows for internal escalation and timeouts of items based on a parameters the user can specify during submission combined with how frequently you ask GPQ to prioritize the queue.
</h4>


## Table of Contents
1. [Table of Contents](https://github.com/JustinTimperio/gpq?tab=readme-ov-file#table-of-contents)
2. [Background](https://github.com/JustinTimperio/gpq?tab=readme-ov-file#background)
4. [Benchmarks](https://github.com/JustinTimperio/gpq?tab=readme-ov-file#benchmarks)
3. [Usage](https://github.com/JustinTimperio/gpq?tab=readme-ov-file#usage)
5. [Contributing](https://github.com/JustinTimperio/gpq?tab=readme-ov-file#contributing)
6. [License](https://github.com/JustinTimperio/gpq?tab=readme-ov-file#license)

## Background
GPQ was written as an experiment when I was playing with [Fibonacci Heaps](https://en.wikipedia.org/wiki/Fibonacci_heap) and wanted to find something faster. I was disappointed by the state of research and libraries being used by most common applications, so GPQ is meant to be a highly flexible framework that can support a multitude of workloads.

### Other Priority Queues I'm Working On
- [fibheap (Fibonacci Heaps)](https://github.com/JustinTimperio/fibheap)
- [gpq (Go Priority Queue)](https://github.com/JustinTimperio/gpq)
- [rpq (Rust Priority Queue)](https://github.com/JustinTimperio/rpq)

## Benchmarks
Due to the fact that most operations are done in constant time `O(1)` or logarithmic time `O(log n)`, with the exception of the prioritize function which happens in linear time `O(n)`, all GPQ operations are extremely fast. A single GPQ can handle a few million transactions a second and can be tuned depending on your work load. I have included some basic benchmarks using C++, Rust, and Go to measure GPQ's performance against the standard implementations of other languages. **While not a direct comparison, 10 million entries fully enqueued and dequeued takes about 3 seconds with Go/GPQ, 3 seconds with Rust, and about 8 seconds for C++**. (Happy to have someone who knows C++ or Rust comment here)


<p align="center">
  <img src="./docs/Reprioritize-All-Buckets-Every-100-Milliseconds-VS-No-Reprioritze.png">
  <img src="./docs/Queue-Speed-WITH-Reprioritize.png">
  <img src="./docs/Queue-Speed-WITHOUT-Reprioritize.png">
</p>



## Usage

### Prerequisites 
For this you will need Go >= `1.22` and gpq itself uses [hashmap](https://github.com/cornelk/hashmap) and [BadgerDB](https://github.com/dgraph-io/badger). 

### Import Directly
GPQ is primarily a embeddable priority queue meant to be used at the core of critical workloads that require complex queueing and delivery order guarantees. The best way to use it is just to import it.

```go
import "github.com/JustinTimperio/gpq"
```


### API Reference
1. `NewGPQ[d any](NumOfBuckets int) *GPQ[d]` - Creates a new GPQ with n number of buckets 
   1. `EnQueue(data d, priorityBucket int64, escalationRate time.Duration) error` - Adds a piece of data into the queue with a priority and escalation rate 
   2. `DeQueue() (priority int64, data d, err error)` - Retrieves the highest priority item in the queue along with its priority
   3. `Prioritize() (uint64, []error)` - Prioritize stops transactions on each bucket concurrently to shuffle the priorities internally within the bucket depending on the escalation rate given at time of EnQueue'ing


### Submitting Items to the Queue
Once you have an initialized queue you can easily submit items like the following:
```go

queue := gpq.NewGPQ[int](10, false, "/path/for/disk/sync")

var (
	data int = 1
	priority int64 = 5 
	shouldEscalate bool = true
	escalationRate time.Duration = time.Duration(time.Second)
	canTimeout bool = true
	timeout time.Duration = time.Duration(10*time.Second)
)

queue.EnQueue(data, priority, shouldEscalate, escalationRate, canTimeout, timeout)

```

You have a few options when you submit a job such as if the item should escalate over time if not sent, or inversely can timeout if it has been enqueued to long to be relevant anymore.

### Example Usage
```go
package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq"
)

type TestStruct struct {
	ID   int
	Name string
}

func main() {

	var (
		total    	int  = 1000
		print    	bool = false
		syncToDisk  bool = true
		retries     int  = 10
		sent     	uint64
		received 	uint64
	 	missed 	 	int64
	 	hits     	int64
	)
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
```


## Contributing
GPQ is actively looking for maintainers so feel free to help out when:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features

### We Develop with Github
We use github to host code, to track issues and feature requests, as well as accept pull requests.

### All Code Changes Happen Through Pull Requests
1. Fork the repo and create your branch from `master`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

### Any contributions you make will be under the MIT Software License
In short, when you submit code changes, your submissions are understood to be under the same [MIT License](http://choosealicense.com/licenses/mit/) that covers the project. Feel free to contact the maintainers if that's a concern.

### Report bugs using Github's [Issues](https://github.com/JustinTimperio/gpq/issues)
We use GitHub issues to track public bugs. Report a bug by opening a new issue; it's that easy!

### Write bug reports with detail, background, and sample code
**Great Bug Reports** tend to have:

- A quick summary and/or background
- Steps to reproduce
  - Be specific!
  - Give sample code if you can.
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

### License
By contributing, you agree that your contributions will be licensed under its MIT License.

## License
All code here was originally written by me, Justin Timperio, under an MIT license with the exception of some code directly forked under a BSD license from the Go maintainers.