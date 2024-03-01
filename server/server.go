package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
	"github.com/JustinTimperio/gpq/server/routes"
	"github.com/JustinTimperio/gpq/server/settings"
	"github.com/dgraph-io/badger/v4"
	"golang.org/x/crypto/bcrypt"

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
		ValidTokens:    hashmap.New[string, schema.Token](),
		SettingsDB:     SettingsDB,
		Logger:         logger,
	}

	// Rebuild the GPQs and ValidTokens from the SettingsDB
	rebuildFromDB(&gpqs)

	// Add the admin user to the settings DB
	// This is done every boot to ensure the password from the config is always used
	if settings.Settings.AdminPass == "" {
		logger.Fatalw("Admin password not set")
	}
	// Hash the admin password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(settings.Settings.AdminPass), bcrypt.DefaultCost)
	if err != nil {
		logger.Fatalw("Failed to hash admin password", "error", err)
	}
	// Add the admin user to the settings DB
	err = gpqs.SettingsDB.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("auth.username." + settings.Settings.AdminUser))
		if err == nil {
			return nil
		}
		return txn.Set([]byte("auth.username."+settings.Settings.AdminUser), hashedPassword)
	})
	if err != nil {
		logger.Fatalw("Failed to set admin user", "error", err)
	}

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

	// Public Routes
	e.POST("/auth", gpqs.Auth)

	// Define Route Groups
	TopicRoutes := e.Group("/topic")
	ManagementRoutes := e.Group("/management")
	SettingsRoutes := e.Group("/settings")

	// Topic Routes
	if settings.Settings.AuthTopics {
		TopicRoutes.Use(routes.GenerateAuthMiddleWare(gpqs))
	}
	TopicRoutes.GET("/list", gpqs.ListTopics)
	TopicRoutes.POST("/:name/enqueue", gpqs.Enqueue)
	TopicRoutes.GET("/:name/dequeue", gpqs.Dequeue)

	// Topic Management Routes
	if settings.Settings.AuthManagement {
		ManagementRoutes.Use(routes.GenerateAuthMiddleWare(gpqs))
	}
	ManagementRoutes.POST("/add_topic", gpqs.AddTopic)
	ManagementRoutes.POST("/remove_topic", gpqs.RemoveTopic)

	// User Management Routes
	// These routes are only accessible by the admin user
	if settings.Settings.AuthSettings {
		SettingsRoutes.Use(routes.GenerateAdminMiddleWare(gpqs))
	}
	SettingsRoutes.POST("/add_user", gpqs.AddUser)
	SettingsRoutes.POST("/remove_user", gpqs.RemoveUser)

	// Finally, start the server
	logger.Infow("Server starting...", "port", settings.Settings.Port, "host_name", settings.Settings.HostName)
	e.Logger.Fatal(e.Start(settings.Settings.HostName + ":" + fmt.Sprintf("%d", settings.Settings.Port)))

}

// rebuildFromDB rebuilds the GPQs and ValidTokens from the SettingsDB
func rebuildFromDB(gpqs *routes.RouteHandler) {
	// Rebuild active tokens from the SettingsDB
	err := gpqs.SettingsDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("auth.token.")

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			gpqs.Logger.Infow("Rebuilding token", "key", string(item.Key()))
			err := item.Value(func(v []byte) error {
				// Decode the token
				var token schema.Token
				var buf bytes.Buffer
				buf.Write(v)
				err := gob.NewDecoder(&buf).Decode(&token)
				if err != nil {
					return err
				}

				if token.Timeout.Before(time.Now()) {
					// Remove the token from the valid tokens map and the database
					err := gpqs.SettingsDB.Update(func(txn *badger.Txn) error {
						err := txn.Delete([]byte("auth.token." + token.Token))
						return err
					})
					if err != nil {
						return err
					}

				} else {
					gpqs.ValidTokens.Set(token.Token, token)
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	},
	)
	if err != nil {
		gpqs.Logger.Fatalw("Failed to rebuild tokens", "error", err)
	}

	// Rebuild the Topics from the SettingsDB
	err = gpqs.SettingsDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("topic.settings.")

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			gpqs.Logger.Infow("Rebuilding topic", "key", string(it.Item().Key()))
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
				go routes.Prioritize(topic.Name, gpqs)

				return nil
			})
			if err != nil {
				return err
			}

		}
		return nil
	})
	if err != nil {
		gpqs.Logger.Fatalw("Failed to rebuild topics", "error", err)
	}
}
