package routes

import (
	"encoding/json"
	"net/http"

	"github.com/JustinTimperio/gpq/server/schema"
	"github.com/JustinTimperio/gpq/server/settings"

	"github.com/dgraph-io/badger/v4"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"
)

// @Summary		Add User
// @Description	Adds a user to the database
// @Tags			Users
// @ID				users-add
// @Accept			json
// @Produce		json
// @Param			credentials	body		schema.Credentials	true	"User Credentials"
// @Success		200			{string}	string				"User added"
// @Failure		400			{string}	string				"Bad Request"
// @Failure		500			{string}	string				"Internal Server Error"
// @Router			/settings/user/add [post]
// @Security		ApiKeyAuth
// @Param			Authorization	header	string	true	"Bearer {token}"
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

// @Summary		Remove User
// @Description	Removes a user from the database
// @Tags			Users
// @ID				users-remove
// @Accept			json
// @Produce		json
// @Param			username	path		string	true	"Username"
// @Success		200			{string}	string	"User removed"
// @Failure		400			{string}	string	"Bad Request"
// @Failure		403			{string}	string	"Forbidden"
// @Failure		404			{string}	string	"Not Found"
// @Failure		500			{string}	string	"Internal Server Error"
// @Router			/settings/user/remove [delete]
// @Security		ApiKeyAuth
// @Param			Authorization	header	string	true	"Bearer {token}"
func (rt RouteHandler) RemoveUser(c echo.Context) error {
	rt.SettingsDB.Update(func(txn *badger.Txn) error {

		// Check if the user is the admin
		if settings.Settings.AdminUser == c.Param("username") {
			return echo.ErrForbidden
		}

		// Remove tokens associated with the user
		rt.ValidTokens.Range(func(k string, v schema.Token) bool {
			if v.Username == c.Param("username") {
				rt.ValidTokens.Del(k)
			}
			return true
		})

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
