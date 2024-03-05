package routes

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/server/schema"
	"github.com/dgraph-io/badger/v4"
	"github.com/labstack/echo/v4"
)

//	@Summary		Add Topic
//	@Description	Adds a topic to the gpq server
//	@Tags			Topics
//	@ID				add-topic
//	@Accept			json
//	@Produce		json
//	@Param			name				query		string	true	"Topic Name"
//	@Param			disk_path			query		string	true	"Disk Path"
//	@Param			buckets				query		int		true	"Buckets"
//	@Param			sync_to_disk		query		bool	true	"Sync To Disk"
//	@Param			reprioritize		query		bool	true	"Reprioritize"
//	@Param			reprioritize_rate	query		string	true	"Reprioritize Rate"
//	@Success		200					{string}	string	"OK"
//	@Failure		400					{string}	string	"Bad Request"
//	@Router			/topic/add [post]
//	@Security		ApiKeyAuth
//	@Param			Authorization	header	string	true	"Bearer {token}"
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

	// Verify that the name is not a reserved word
	for _, word := range RESERVEDKEYSPREFIXES {
		if strings.Contains(topic.Name, word) {
			return echo.NewHTTPError(400, "Topic name contains a reserved word")
		}
	}

	// Create a new GPQ
	queue, err := gpq.NewGPQ[[]byte](topic.Buckets, topic.SyncToDisk, topic.DiskPath)
	if err != nil {
		return echo.NewHTTPError(500, "Failed to create queue")
	}

	// Add the queue to the hashmap
	rt.Topics.Set(topic.Name, queue)
	rt.TopicsSettings.Set(topic.Name, topic)

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

//	@Summary		Remove Topic
//	@Description	Removes a topic from the gpq server
//	@Tags			Topics
//	@ID				remove-topic
//	@Accept			json
//	@Produce		json
//	@Param			name	query		string	true	"Topic Name"
//	@Success		200		{string}	string	"OK"
//	@Failure		400		{string}	string	"Bad Request"
//	@Router			/topic/remove [post]
//	@Security		ApiKeyAuth
//	@Param			Authorization	header	string	true	"Bearer {token}"
func (rt RouteHandler) RemoveTopic(c echo.Context) error {
	name := c.QueryParam("name")
	if name == "" {
		return echo.NewHTTPError(400, "No topic name provided")
	}

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

//	@Summary		List Topics
//	@Description	Lists all topics in the gpq server
//	@Tags			Topics
//	@ID				list-topics
//	@Produce		json
//	@Success		200	{string}	string	"OK"
//	@Failure		400	{string}	string	"Bad Request"
//	@Router			/topic/list [get]
//	@Security		ApiKeyAuth
//	@Param			Authorization	header	string	true	"Bearer {token}"
func (rt RouteHandler) ListTopics(c echo.Context) error {

	topics := make(map[string]uint64)
	rt.Topics.Range(func(key string, value *gpq.GPQ[[]byte]) bool {
		topics[key] = atomic.LoadUint64(&value.NonEmptyBuckets.ObjectsInQueue)
		return true
	})
	return c.JSONPretty(200, topics, "  ")
}
