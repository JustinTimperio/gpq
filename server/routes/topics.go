package routes

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
	"github.com/dgraph-io/badger/v4"
	"github.com/labstack/echo/v4"
)

// AddTopic adds a topic to the hashmap
func (rt *RouteHandler) AddTopic(c echo.Context) error {
	var err error

	// Get the Parameters
	topic := &schema.Topic{}
	topic.Name = c.QueryParam("name")
	if topic.Name == "" {
		return echo.NewHTTPError(400, "No topic name provided")
	}

	topic.DiskPath = c.QueryParam("disk_path")
	if topic.DiskPath == "" {
		return echo.NewHTTPError(400, "No disk path provided")
	}

	topic.Buckets, err = strconv.Atoi(c.QueryParam("buckets"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse buckets")
	}
	topic.SyncToDisk, err = strconv.ParseBool(c.QueryParam("sync_to_disk"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse sync_to_disk")
	}
	topic.RePrioritize, err = strconv.ParseBool(c.QueryParam("reprioritize"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse reprioritize")
	}
	topic.RePrioritizeRate, err = time.ParseDuration(c.QueryParam("reprioritize_rate"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse reprioritize_rate")
	}

	_, exists := rt.Topics.Get(topic.Name)
	if exists {
		return echo.NewHTTPError(400, "Topic already exists")
	}

	// Create a new GPQ
	queue, err := gpq.NewGPQ[[]byte](topic.Buckets, topic.SyncToDisk, topic.DiskPath)
	if err != nil {
		return echo.NewHTTPError(500, "Failed to create queue")
	}

	// Add the queue to the hashmap
	rt.Topics.Set(topic.Name, queue)

	// Start the reprioritization process
	go Prioritize(topic.Name, rt)

	// Encode the item
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(topic)
	if err != nil {
		return echo.NewHTTPError(500, "Failed to encode topic")
	}

	// Send the item to the settings DB
	key := []byte("topic.settings." + topic.Name)
	value := buf.Bytes()

	rt.SettingsDB.Update(func(txn *badger.Txn) error {
		txn.Set(key, value)
		return nil
	})

	return echo.NewHTTPError(200, "Topic added")
}

// RemoveTopic removes a topic from the hashmap
func (rt RouteHandler) RemoveTopic(c echo.Context) error {
	name := c.QueryParam("name")
	_, exists := rt.Topics.Get(name)
	if !exists {
		return echo.NewHTTPError(400, "Topic not found")
	}
	rt.Topics.Del(name)

	key := []byte("topic.settings." + name)

	rt.SettingsDB.Update(func(txn *badger.Txn) error {
		txn.Delete(key)
		return nil
	})

	return echo.NewHTTPError(200, "Topic removed")
}

// ListTopics lists all topics in the hashmap
func (rt RouteHandler) ListTopics(c echo.Context) error {

	topics := make(map[string]int64, rt.Topics.Len())
	rt.Topics.Range(func(key string, value *gpq.GPQ[[]byte]) bool {
		topics[key] = *value.NonEmptyBuckets.Len()
		return true
	})
	return c.JSONPretty(200, topics, "  ")
}
