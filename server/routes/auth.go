package routes

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"net/http"
	"time"

	"github.com/JustinTimperio/gpq/schema"
	"github.com/JustinTimperio/gpq/server/settings"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/crypto/bcrypt"
)

// Auth authenticates a user and returns a token
func (rt *RouteHandler) Auth(c echo.Context) error {
	// Parse and decode the request body into a new `Credentials` instance
	creds := &schema.Credentials{}
	err := json.NewDecoder(c.Request().Body).Decode(creds)
	if err != nil {
		return echo.ErrBadRequest
	}

	// Get the hashed password from the database
	var hashedPassword []byte
	err = rt.SettingsDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("auth.username." + creds.Username))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			hashedPassword = val
			return nil
		})
		return err
	})
	if err != nil {
		return echo.ErrUnauthorized
	}

	// Compare the stored hashed password, with the hashed version of the password that was received
	if err = bcrypt.CompareHashAndPassword(hashedPassword, []byte(creds.Password)); err != nil {
		return echo.ErrUnauthorized
	}

	var isAdmin bool
	if creds.Username == settings.Settings.AdminUser {
		isAdmin = true
	}

	// Create a new token
	token := schema.Token{
		// TODO: Generate a real token instead of a UUID
		Token:    uuid.NewString(),
		Timeout:  time.Now().Add(time.Hour * 24),
		IsAdmin:  isAdmin,
		Username: creds.Username,
	}

	// Store the token in the database and the valid tokens map
	err = rt.SettingsDB.Update(func(txn *badger.Txn) error {

		// Encode the token and store it in the d	// Encode the item
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(token)
		if err != nil {
			return echo.NewHTTPError(500, "Failed to encode token")
		}

		err := txn.Set([]byte("auth.token."+token.Token), buf.Bytes())
		return err
	})
	if err != nil {
		return echo.ErrInternalServerError
	}
	rt.ValidTokens.Set(token.Token, token)

	// Return the token
	return c.JSON(http.StatusOK, token)
}

// generateAuthMiddleWare generates a middleware function that checks for a valid token
func GenerateAuthMiddleWare(gpqs RouteHandler) echo.MiddlewareFunc {

	return middleware.KeyAuth(func(key string, c echo.Context) (bool, error) {
		token, ok := gpqs.ValidTokens.Get(key)
		if !ok {
			return false, echo.ErrForbidden
		}
		if token.Timeout.Before(time.Now()) {
			// Remove the token from the valid tokens map and the database
			err := gpqs.SettingsDB.Update(func(txn *badger.Txn) error {
				err := txn.Delete([]byte("auth.token." + token.Token))
				return err
			})
			if err != nil {
				return false, echo.ErrInternalServerError
			}
			gpqs.ValidTokens.Del(key)
			return false, echo.ErrForbidden
		}
		return ok, nil
	},
	)
}

// generateAdminMiddleWare generates a middleware function that checks for a valid token
func GenerateAdminMiddleWare(gpqs RouteHandler) echo.MiddlewareFunc {
	return middleware.KeyAuth(func(key string, c echo.Context) (bool, error) {

		token, ok := gpqs.ValidTokens.Get(key)
		if !ok {
			return false, echo.ErrForbidden
		}

		if !token.IsAdmin {
			return false, echo.ErrForbidden
		}

		if token.Timeout.Before(time.Now()) {
			// Remove the token from the valid tokens map and the database
			err := gpqs.SettingsDB.Update(func(txn *badger.Txn) error {
				err := txn.Delete([]byte("auth.token." + token.Token))
				return err
			})
			if err != nil {
				return false, echo.ErrInternalServerError
			}
			gpqs.ValidTokens.Del(key)
			return false, echo.ErrForbidden
		}
		return ok, nil
	},
	)

}
