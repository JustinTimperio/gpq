package routes

import (
	"sync"
	"time"

	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/server/schema"

	"github.com/cornelk/hashmap"
	"github.com/dgraph-io/badger/v4"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// Reserved words and keys
var RESERVEDKEYSPREFIXES = []string{
	"topic.",
	"topic.settings.",
	"auth.",
	"auth.username.",
	"auth.token.",
}

type RouteHandler struct {
	WS             *echo.Echo
	Topics         *hashmap.Map[string, *gpq.GPQ[[]byte]]
	TopicsSettings *hashmap.Map[string, *schema.Topic]
	Users          *hashmap.Map[string, *schema.User]
	ValidTokens    *hashmap.Map[string, schema.Token]
	SettingsDB     *badger.DB
	Logger         *zap.SugaredLogger
	WaitForSync    *sync.WaitGroup
}

// Prioritize reprioritizes the GPQ at the specified rate
// It also can dynamically update its own settings
func Prioritize(topicName string, gpqs *RouteHandler) {
	gpqs.Logger.Infow("Starting to reprioritize daemon", "topic", topicName)

	for {

		topic, exists := gpqs.TopicsSettings.Get(topicName)
		if !exists {
			gpqs.Logger.Infow("Topic no longer exists", "topic", topicName)
			return
		}

		// Sleep for the specified duration
		gpqs.Logger.Debugw("Sleeping", "duration", topic.RePrioritizeRate)
		time.Sleep(topic.RePrioritizeRate)

		// Check if the topic still exists
		// If it doesn't, then stop reprioritizing
		gpq, exists := gpqs.Topics.Get(topic.Name)

		if !exists {
			gpqs.Logger.Infow("Topic no longer exists", "topic", topic)
			return
		}

		if topic.RePrioritize {
			// Reprioritize the GPQ
			timedOut, esclatedItems, errs := gpq.Prioritize()

			// Log the results
			gpqs.Logger.Infow("Reprioritized", "topic", topic.Name, "escalated_items", esclatedItems, "timed_out", timedOut)
			for _, err := range errs {
				gpqs.Logger.Debugw("Failed to reprioritize", "error", err)
			}
		}
	}
}
