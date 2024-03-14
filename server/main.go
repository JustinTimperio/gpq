package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/JustinTimperio/gpq"
	gpqschema "github.com/JustinTimperio/gpq/schema"
	"github.com/JustinTimperio/gpq/server/routes"
	"github.com/JustinTimperio/gpq/server/schema"
	"github.com/JustinTimperio/gpq/server/settings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/cornelk/hashmap"
	"github.com/dgraph-io/badger/v4"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"

	_ "github.com/JustinTimperio/gpq/server/docs"
	echoSwagger "github.com/swaggo/echo-swagger"
)

// @title			Swagger GPQ API
// @version		1.0
// @description	This is the API for the GPQ (Go Priority Queue) server.
// @license.name	MIT License
// @host			localhost:4040
// @BasePath		/v1
func main() {

	// Capture SIGINT and SIGTERM
	interupt := make(chan os.Signal, 1)
	signal.Notify(interupt, syscall.SIGINT, syscall.SIGTERM)

	// Gob Register
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	// Register PrimitiveTypes for gob
	PrimitiveTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
		arrow.PrimitiveTypes.Date32,
		arrow.PrimitiveTypes.Date64,
	}

	for _, v := range PrimitiveTypes {
		gob.Register(v)
	}
	// Create a Zap Logger
	cfg := zap.NewProductionConfig()

	if settings.Settings.STDOut {
		cfg.OutputPaths = []string{"stdout", settings.Settings.LogPath}
	} else {
		cfg.OutputPaths = []string{settings.Settings.LogPath}
	}

	logger := zap.Must(cfg.Build()).Sugar()
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
		WaitForSync:    &sync.WaitGroup{},
	}

	// Rebuild the GPQs and ValidTokens from the SettingsDB
	rebuildFromDB(&gpqs)

	// Launch a cleanup routine to in the case that the server is restarted
	go func() {
		gpqs.WaitForSync.Add(1)
		defer gpqs.WaitForSync.Done()
		// Wait for Interupt
		<-interupt

		gpqs.Logger.Infow("Server shutting down topics and syncing disk...")

		// Shutdown the server gracefully
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		gpqs.WS.Shutdown(ctx)

		// Close the GPQs
		gpqs.Topics.Range(func(key string, gpq *gpq.GPQ[[]byte]) bool {
			gpq.Close()
			return true
		})

		// Sync the SettingsDB
		gpqs.SettingsDB.Sync()
		gpqs.SettingsDB.Close()
	}()

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

			logger.Debugw("request",
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
	e.GET("/swagger/*", echoSwagger.WrapHandler)

	// Define Route Groups
	TopicRoutes := e.Group("/topic")
	ManagementRoutes := e.Group("/management")
	SettingsRoutes := e.Group("/settings")
	e.GET("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))

	// Topic Routes
	if settings.Settings.AuthTopics {
		TopicRoutes.Use(routes.GenerateAuthMiddleWare(gpqs))
	}
	TopicRoutes.POST("/:name/raw/enqueue", gpqs.RawReceive)
	TopicRoutes.GET("/:name/raw/dequeue", gpqs.RawServe)
	TopicRoutes.POST("/:name/arrow/enqueue", gpqs.ArrowReceive)
	TopicRoutes.GET("/:name/arrow/dequeue", gpqs.ArrowServe)
	TopicRoutes.POST("/:name/avro/enqueue", gpqs.AvroReceive)
	TopicRoutes.GET("/:name/avro/dequeue", gpqs.AvroServe)

	// Topic Management Routes
	if settings.Settings.AuthManagement {
		ManagementRoutes.Use(routes.GenerateAuthMiddleWare(gpqs))
	}
	ManagementRoutes.GET("/topic/list", gpqs.ListTopics)
	ManagementRoutes.POST("/topic/add", gpqs.AddTopic)
	ManagementRoutes.POST("/topic/remove", gpqs.RemoveTopic)

	// User Management Routes
	// These routes are only accessible by the admin user
	if settings.Settings.AuthSettings {
		SettingsRoutes.Use(routes.GenerateAdminMiddleWare(gpqs))
	}
	SettingsRoutes.POST("/user/add", gpqs.AddUser)
	SettingsRoutes.POST("/user/remover", gpqs.RemoveUser)

	// Finally, start the server
	logger.Infow("Server starting...", "port", settings.Settings.Port, "host_name", settings.Settings.HostName)
	e.Logger.Info(e.Start(settings.Settings.HostName + ":" + fmt.Sprintf("%d", settings.Settings.Port)))
	gpqs.WaitForSync.Wait()

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

				opts := gpqschema.GPQOptions{
					NumberOfBatches:       topic.Buckets,
					DiskCacheEnabled:      topic.SyncToDisk,
					DiskCachePath:         topic.DiskPath,
					DiskCacheCompression:  settings.Settings.DiskCacheCompression,
					DiskEncryptionEnabled: settings.Settings.DiskEncryptionEnabled,
					DiskEncryptionKey:     []byte(settings.Settings.DiskEncryptionKey),
					LazyDiskCacheEnabled:  topic.LazyDiskSync,
					LazyDiskBatchSize:     int(topic.BatchSize),
				}

				// Create a new GPQ
				_, queue, err := gpq.NewGPQ[[]byte](opts)
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
