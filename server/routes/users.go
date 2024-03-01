package routes

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/JustinTimperio/gpq/schema"
	"github.com/JustinTimperio/gpq/server/settings"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"
)

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

	// Create a new token
	token := schema.Token{
		// TODO: Generate a real token instead of a UUID
		Token:   uuid.NewString(),
		Timeout: time.Now().Add(time.Hour * 24),
	}

	// Store the token in the database
	rt.ValidTokens.Set(token.Token, token)

	// Return the token
	return c.JSON(http.StatusOK, token)
}

func (rt RouteHandler) AddUser(c echo.Context) error {
	// Parse and decode the request body into a new `Credentials` instance
	creds := &schema.Credentials{}
	err := json.NewDecoder(c.Request().Body).Decode(creds)
	if err != nil {
		return echo.ErrBadRequest
	}
	// Salt and hash the password using the bcrypt algorithm
	// The second argument is the cost of hashing
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(creds.Password), bcrypt.DefaultCost)
	if err != nil {
		return echo.ErrInternalServerError
	}

	// Store the hashed password in the database
	rt.SettingsDB.Update(func(txn *badger.Txn) error {
		uname := []byte("auth.username." + creds.Username)
		if err := txn.Set(uname, hashedPassword); err != nil {
			return err
		}
		return nil
	})
	return c.JSON(http.StatusOK, "User added")
}

func (rt RouteHandler) RemoveUser(c echo.Context) error {
	rt.SettingsDB.Update(func(txn *badger.Txn) error {

		// Check if the user is the admin
		if settings.Settings.AdminUser == c.Param("username") {
			return echo.ErrForbidden
		}

		// TODO: Remove tokens associated with the user

		// Get the user from the database
		_, err := txn.Get([]byte("auth.username." + c.Param("username")))
		if err != nil {
			return echo.ErrNotFound
		}
		// Delete the user from the database
		err = txn.Delete([]byte("auth.username." + c.Param("username")))
		if err != nil {
			return echo.ErrInternalServerError
		}

		return nil
	})
	return c.JSON(http.StatusOK, "User removed")
}
