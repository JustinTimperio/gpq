package gpq

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/JustinTimperio/gpq/fsm"
	"github.com/JustinTimperio/gpq/schema"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// GPQ is a generic priority queue that supports priority levels and timeouts
// It is implemented using a heap for each priority level and a priority queue of non-empty buckets
// It also supports disk caching using badgerDB with the option to lazily disk writes and deletes
// The GPQ is thread-safe and supports concurrent access
type GPQ[d any] struct {
	// Raft is the raft consensus protocol
	Raft *raft.Raft
	// Options is the options for the GPQ
	Options schema.GPQOptions
	// FSM is the finite state machine for the GPQ
	FSM *fsm.FSM[d]
	// StableStore is the stable store for the GPQ
	StableStore *raftboltdb.BoltStore
	// CacheStore is the cache store for the GPQ
	CacheStore *raft.LogCache
	// SnapshotStore is the snapshot store for the GPQ
	SnapshotStore *raft.FileSnapshotStore
	// Transport is the transport for the GPQ
	Transport *raft.NetworkTransport
}

// NewGPQ creates a new GPQ with the given number of buckets
// The number of buckets is the number of priority levels you want to support
// You must provide the number of buckets ahead of time and all priorities you submit
// must be within the range of 0 to NumOfBuckets
func NewGPQ[d any](options schema.GPQOptions) (*GPQ[d], error) {

	fmt.Println(options)

	os.MkdirAll(options.RaftSnapshotPath, 0755)
	os.Mkdir(options.DiskCachePath, 0755)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(options.RaftNodeID)

	gpq := &GPQ[d]{
		Options: options,
	}

	var err error
	gpq.FSM, err = fsm.NewFSM[d](gpq.Options)
	if err != nil {
		return nil, err
	}

	gpq.StableStore, err = raftboltdb.NewBoltStore(options.RaftStableStorePath)
	if err != nil {
		return nil, err
	}
	// Wrap the store in a LogCache to improve performance.
	gpq.CacheStore, err = raft.NewLogCache(options.RaftCacheSize, gpq.StableStore)
	if err != nil {
		return nil, err
	}
	gpq.SnapshotStore, err = raft.NewFileSnapshotStore(options.RaftSnapshotPath, options.RaftSnapshotRetain, os.Stdout)
	if err != nil {
		return nil, err
	}
	gpq.Transport, err = raft.NewTCPTransport(fmt.Sprintf("%s:%d", options.RaftBindAddress, options.RaftPort), nil, options.RaftPoolConnections, 30*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	gpq.Raft, err = raft.NewRaft(raftConfig, gpq.FSM, gpq.CacheStore, gpq.StableStore, gpq.SnapshotStore, gpq.Transport)
	if err != nil {
		return nil, err
	}
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(options.RaftNodeID),
				Address: raft.ServerAddress(fmt.Sprintf("%s:%d", options.RaftBindAddress, options.RaftPort)),
			},
		},
	}

	bf := gpq.Raft.BootstrapCluster(configuration)
	if bf.Error() == errors.New("bootstrap only works on new clusters") {
		fmt.Println("Cluster already bootstrapped")
	}

	config := gpq.Raft.GetConfiguration()
	if err := config.Error(); err != nil {
		return nil, fmt.Errorf("Failed to get raft configuration: %v", err)
	}

	if len(config.Configuration().Servers) > 1 {
		return nil, fmt.Errorf("More than one server in the raft configuration")
	}

	/*
		if options.DiskCache {
			if options.DiskCachePath == "" {
				return gpq, errors.New("Disk cache path is required")
			}
			opts := badger.DefaultOptions(options.DiskCachePath)
			opts.Logger = nil
			db, err := badger.Open(opts)
			if err != nil {
				return nil, errors.New("Error opening disk cache: " + err.Error())
			}
			gpq.DiskCache = db
			var reEnqueued uint64

			// Re-add items to the GPQ from the disk cache
			err = gpq.DiskCache.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()

				for it.Rewind(); it.Valid(); it.Next() {
					var value []byte
					item := it.Item()
					key := item.Key()

					// Get the item from the disk cache
					item, err := txn.Get(key)
					if err != nil {
						return err
					}
					item.Value(func(val []byte) error {
						value = append([]byte{}, val...)
						return nil
					})

					if len(value) == 0 {
						return errors.New("Error reading from disk cache: value is empty")
					}

					// Decode the item
					var buf bytes.Buffer
					buf.Write(value)
					obj := schema.Item[d]{}
					err = gob.NewDecoder(&buf).Decode(&obj)
					if err != nil {
						return errors.New("Error decoding item from disk cache: " + err.Error())
					}

					// Re-enqueue the item with the same parameters it had when it was enqueued
					err = gpq.reQueue(obj.Data, obj.Priority, obj.ShouldEscalate, obj.EscalationRate, obj.CanTimeout, obj.Timeout, obj.DiskUUID)
					if err != nil {
						return err
					}

					reEnqueued++
				}

				return nil
			})
			if err != nil {
				return nil, errors.New("Error reading from disk cache: " + err.Error())
			}

		} else {
			gpq.DiskCache = nil
		}
	*/

	return gpq, nil
}

// EnQueue adds an item to the GPQ
// The priorityBucket is the priority level of the item
// The escalationRate is the amount of time before the item is escalated to the next priority level
// The data is the data you want to store in the GPQ item
func (g *GPQ[d]) EnQueue(data d, priorityBucket int64, escalate bool, escalationRate time.Duration, canTimeout bool, timeout time.Duration, lazyDiskComit bool) error {

	if priorityBucket > int64(g.Options.NumberOfBuckets) {
		return errors.New("Priority bucket does not exist")
	}

	// Generate a UUID for the item
	key, err := uuid.New().MarshalBinary()
	if err != nil {
		return err
	}

	obj := schema.Item[d]{
		Data:           data,
		Priority:       priorityBucket,
		ShouldEscalate: escalate,
		EscalationRate: escalationRate,
		CanTimeout:     canTimeout,
		Timeout:        timeout,
		SubmittedAt:    time.Now(),
		LastEscalated:  time.Now(),
		DiskUUID:       key,
	}

	var rbuf bytes.Buffer
	enc := gob.NewEncoder(&rbuf)
	err = enc.Encode(fsm.CommandPayload[d]{
		Operation:     fsm.EnQueue,
		Data:          obj,
		LazyOperation: lazyDiskComit,
	})
	if err != nil {
		return err
	}

	future := g.Raft.Apply(rbuf.Bytes(), time.Duration(10)*time.Second)
	resp := future.Response()
	if resp == nil {
		return future.Error()
	}

	r, ok := resp.(fsm.ApplyResponse[d])
	if !ok {
		return fmt.Errorf("Error converting response to ApplyResponse: %v", resp)
	}

	if r.Err != nil {
		return fmt.Errorf("Failed to apply item to raft! ResponseError: %v", r.Err)
	}

	return nil
}

/*
// reQueue adds an item to the GPQ with a specific key restored from the disk cache
func (g *GPQ[d]) reQueue(data d, priorityBucket int64, escalate bool, escalationRate time.Duration, canTimeout bool, timeout time.Duration, key []byte) error {

	if priorityBucket > g.BucketCount {
		return errors.New("Priority bucket does not exist")
	}

	// Create the item
	obj := schema.Item[d]{
		Data:           data,
		Priority:       priorityBucket,
		ShouldEscalate: escalate,
		EscalationRate: escalationRate,
		CanTimeout:     canTimeout,
		Timeout:        timeout,
		SubmittedAt:    time.Now(),
		LastEscalated:  time.Now(),
	}

	pq, _ := g.Buckets.Get(priorityBucket)

	// Send the item to the disk cache
	obj.DiskUUID = key

	pq.EnQueue(obj)

	return nil
}
*/

// DeQueue removes and returns the item with the highest priority from the GPQ.
// It returns the priority of the item, the data associated with it,
// and an error if the queue is empty or if any internal data structures are missing.
func (g *GPQ[d]) DeQueue() (priority int64, data d, err error) {

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(fsm.CommandPayload[d]{
		Operation: fsm.DeQueue,
	})
	if err != nil {
		return -1, data, err
	}

	future := g.Raft.Apply(buf.Bytes(), time.Duration(10)*time.Second)
	resp := future.Response()
	if resp == nil {
		return -1, data, future.Error()
	}
	r, ok := resp.(fsm.ApplyResponse[d])
	if !ok {
		return -1, data, fmt.Errorf("Error converting response to ApplyResponse: %v", resp)
	}

	if r.Err != nil {
		return -1, data, fmt.Errorf("Failed to apply item to raft! ResponseError: %v", r.Err)
	}

	return r.Priority, r.Data, nil

}

// Peek returns the item with the highest priority from the GPQ.
// It returns the priority of the item, the data associated with it,
// and an error if the queue is empty or if any internal data structures are missing.
func (g *GPQ[d]) Peek() (data d, err error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(fsm.CommandPayload[d]{
		Operation: fsm.Peek,
	})
	if err != nil {
		return data, err
	}

	future := g.Raft.Apply(buf.Bytes(), time.Duration(10)*time.Second)

	resp, ok := future.Response().(fsm.ApplyResponse[d])
	if !ok {
		return data, errors.New("Failed to apply item to raft")
	}
	if resp.Err != nil {
		return data, resp.Err
	}

	return resp.Data, nil

}

/*
// Prioritize is a method of the GPQ type that prioritizes items within a heap.
// It iterates over each bucket in the GPQ, locks the corresponding mutex, and checks if there are items to prioritize.
// If there are items, it calculates the number of durations that have passed since the last escalation and updates the priority accordingly.
// The method uses goroutines to process each bucket concurrently, improving performance.
// It returns an error if any of the required data structures are missing or if there are no items to prioritize.
func (g *GPQ[d]) Prioritize() (timedOutItems uint64, escalatedItems uint64, errs []error) {

	for bucketID := 0; bucketID < int(g.BucketCount); bucketID++ {

		pq, _ := g.Buckets.Get(int64(bucketID))
		mutex, _ := g.BucketPrioritizeLockMap.Get(int64(bucketID))
		mutex.Lock()
		pointers := pq.ReadPointers()

		if len(pointers) == 0 {
			errs = append(errs, errors.New("No items to prioritize in heap: "+fmt.Sprintf("%d", bucketID)))
			mutex.Unlock()
			continue
		}

		evalTime := time.Now()
		for _, pointer := range pointers {

			if pointer == nil {
				continue
			}

			// Remove the item if it has timed out
			if pointer.CanTimeout {
				duration := int(math.Abs(float64(pointer.SubmittedAt.Sub(evalTime).Milliseconds())))
				if duration > int(pointer.Timeout.Milliseconds()) {

					// Remove the item from the priority queue
					pq.Remove(pointer)
					atomic.AddUint64(&timedOutItems, 1)

					// Remove the item from the disk cache
					if g.DiskCacheEnabled {
						g.ActiveDBSessions.Add(1)
						go func() {
							defer g.ActiveDBSessions.Done()
							g.DiskCache.Update(func(txn *badger.Txn) error {
								return txn.Delete(pointer.DiskUUID)
							})
						}()

						continue
					}
				}
			}

			// Escalate the priority if the item hasn't timed out and can escalate
			if pointer.ShouldEscalate {
				// Calculate the number of durations that fit between evalTime and pointer.SubmittedAt
				duration := int(math.Abs(float64(pointer.LastEscalated.Sub(evalTime).Milliseconds())))
				numDurations := duration / int(pointer.EscalationRate.Milliseconds())

				// If the number of durations is greater than 0, escalate the priority
				if numDurations > 0 {
					pointer.Priority = pointer.Priority - int64(numDurations)
					if pointer.Priority < 0 {
						pointer.Priority = 0
					}
					pointer.LastEscalated = evalTime
					pq.UpdatePriority(pointer, pointer.Priority)
					atomic.AddUint64(&escalatedItems, 1)
				}
			}
		}
		mutex.Unlock()
	}

	return timedOutItems, escalatedItems, errs
}
*/

// Close closes the GPQ and all associated data stores
func (g *GPQ[d]) Close() error {
	err := g.Raft.Shutdown().Error()
	if err != nil {
		return err
	}
	err = g.StableStore.Close()
	if err != nil {
		return err
	}
	err = g.Transport.Close()
	if err != nil {
		return err
	}
	g.FSM.Close()

	return nil
}
