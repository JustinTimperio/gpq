package main

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
	"github.com/JustinTimperio/gpq/server/routes"
	"github.com/JustinTimperio/gpq/server/settings"
	"github.com/dgraph-io/badger/v4"

	"github.com/cornelk/hashmap"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
)

func main() {

	// Create a Zap Logger
	logger := zap.Must(zap.NewProduction()).Sugar()
	defer logger.Sync()

	// Load the Settings
	if err := settings.LoadSettings(); err != nil {
		logger.Fatalw("Failed to load settings", "error", err)
	}

	// Open the Settings BadgerDB
	opts := badger.DefaultOptions(settings.Settings.SettingsDBPath)
	SettingsDB, err := badger.Open(opts)
	if err != nil {
		logger.Fatalw("Failed to open settings DB", "error", err)
	}

	// Create the Server to Receive Requests and Send Responses
	e := echo.New()
	gpqs := routes.RouteHandler{
		WS:             e,
		Topics:         hashmap.New[string, *gpq.GPQ[[]byte]](),
		TopicsSettings: hashmap.New[string, *schema.Topic](),
		Users:          hashmap.New[string, *schema.User](),
		ValidTokens:    hashmap.New[string, bool](),
		SettingsDB:     SettingsDB,
		Logger:         logger,
	}

	// Rebuild the Topics from the SettingsDB
	err = gpqs.SettingsDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("topic.settings.")

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {

				// Decode the topic
				var topic schema.Topic
				var buf bytes.Buffer
				buf.Write(v)
				err = gob.NewDecoder(&buf).Decode(&topic)
				if err != nil {
					return err
				}

				// Create a new GPQ
				queue, err := gpq.NewGPQ[[]byte](topic.Buckets, topic.SyncToDisk, topic.DiskPath)
				if err != nil {
					return err
				}

				// Add the queue to the hashmap
				gpqs.Topics.Set(topic.Name, queue)
				gpqs.TopicsSettings.Set(topic.Name, &topic)

				// Start the reprioritization process
				go routes.Prioritize(topic.Name, &gpqs)

				return nil
			})
			if err != nil {
				return err
			}

		}
		return nil
	})
	if err != nil {
		logger.Fatalw("Failed to rebuild topics", "error", err)
	}

	// Topic Routes
	e.POST("/topic/:name/enqueue", gpqs.Enqueue)
	e.GET("/topic/:name/dequeue", gpqs.Dequeue)

	// Topic Management Routes
	e.GET("/settings/list_topics", gpqs.ListTopics)
	e.POST("/settings/add_topic", gpqs.AddTopic)
	e.POST("/settings/remove_topic", gpqs.RemoveTopic)

	// User Management Routes
	e.POST("/settings/add_user", gpqs.AddUser)
	e.POST("/settings/remove_user", gpqs.RemoveUser)
	e.POST("/settings/update_user", gpqs.UpdateUser)

	// Use Zap for Logging
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogURI:    true,
		LogStatus: true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {

			logger.Infow("request",
				"URI", v.URI,
				"status", v.Status,
				"latency", v.Latency,
				"remote_ip", v.RemoteIP,
				"host", v.Host,
				"method", v.Method,
				"referer", v.Referer,
				"user_agent", v.UserAgent,
				"error", v.Error,
			)
			return nil
		},
	}))

	// Finally, start the server
	logger.Infow("Server starting...", "port", settings.Settings.Port, "host_name", settings.Settings.HostName)
	e.Logger.Fatal(e.Start(settings.Settings.HostName + ":" + fmt.Sprintf("%d", settings.Settings.Port)))

}
