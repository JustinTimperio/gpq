package routes

import (
	"github.com/JustinTimperio/gpq"
	"github.com/JustinTimperio/gpq/schema"
	"github.com/cornelk/hashmap"
	"github.com/dgraph-io/badger/v4"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// Reserved words and keys
var RESERVEDKEYSPREFIXES = []string{
	"settings.topic.",
	"settings.user.",
}

type RouteHandler struct {
	WS             *echo.Echo
	Topics         *hashmap.Map[string, *gpq.GPQ[[]byte]]
	TopicsSettings *hashmap.Map[string, *schema.Topic]
	Users          *hashmap.Map[string, *schema.User]
	ValidTokens    *hashmap.Map[string, bool]
	SettingsDB     *badger.DB
	Logger         *zap.SugaredLogger
}
