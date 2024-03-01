package routes

import (
	"time"

	"github.com/JustinTimperio/gpq/server/settings"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/crypto/bcrypt"
)

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
			esclatedItems, timedOut, errs := gpq.Prioritize()

			// Log the results
			gpqs.Logger.Infow("Reprioritized", "topic", topic.Name, "escalated_items", esclatedItems, "timed_out", timedOut)
			for _, err := range errs {
				gpqs.Logger.Debugw("Failed to reprioritize", "error", err)
			}
		}
	}
}

// generateAuthMiddleWare generates a middleware function that checks for a valid token
func GenerateAuthMiddleWare(gpqs RouteHandler) echo.MiddlewareFunc {

	return middleware.KeyAuth(func(key string, c echo.Context) (bool, error) {
		token, ok := gpqs.ValidTokens.Get(key)
		if !ok {
			return false, echo.ErrForbidden
		}
		if token.Timeout.Before(time.Now()) {
			gpqs.ValidTokens.Del(key)
			return false, echo.ErrForbidden
		}
		return ok, nil
	},
	)
}

// generateAdminMiddleWare generates a middleware function that checks for a valid token
func GenerateAdminMiddleWare(gpqs RouteHandler) echo.MiddlewareFunc {
	return middleware.BasicAuth(func(username, password string, c echo.Context) (bool, error) {

		gpqs.Logger.Infow("Admin login attempt", "username", username)

		if username != settings.Settings.AdminUser {
			return false, echo.ErrForbidden
		}

		token, ok := gpqs.ValidTokens.Get(username)
		if !ok {
			return false, echo.ErrForbidden
		}

		if err := bcrypt.CompareHashAndPassword([]byte(token.Token), []byte(password)); err != nil {
			return false, echo.ErrUnauthorized
		}

		if token.Timeout.Before(time.Now()) {
			gpqs.ValidTokens.Del(username)
			return false, echo.ErrForbidden
		}
		return ok, nil

	})
}
