package routes

import (
	"io"
	"strconv"
	"time"

	cschema "github.com/JustinTimperio/gpq/schema"

	"github.com/labstack/echo/v4"
)

// @Summary		Receive Raw Data
// @Description	Receives Raw data and enqueues it into a topic
// @Tags			Raw
// @ID				enqueue-raw
// @Accept			application/octet-stream
// @Produce		json
// @Param			name				path		string	true	"Topic Name"
// @Param			priority			query		int		true	"Priority"
// @Param			should_escalate		query		bool	true	"Should Escalate"
// @Param			escalate_every		query		string	true	"Escalate Every"
// @Param			can_timeout			query		bool	true	"Can Timeout"
// @Param			timeout_duration	query		string	true	"Timeout Duration"
// @Success		200					{string}	string	"OK"
// @Failure		400					{string}	string	"Bad Request"
// @Router			/topic/{name}/raw/enqueue [post]
// @Security		ApiKeyAuth
// @Param			Authorization	header	string	true	"Bearer {token}"
func (rt RouteHandler) RawReceive(c echo.Context) error {
	// Get the Queue
	name := c.Param("name")
	if name == "" {
		return echo.NewHTTPError(400, "No topic name provided")
	}
	queue, exists := rt.Topics.Get(name)
	if !exists {
		return echo.NewHTTPError(400, "Topic not found")
	}

	// Get the Message Properties
	priority, err := strconv.Atoi(c.QueryParam("priority"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse priority")
	}
	shouldEscalate, err := strconv.ParseBool(c.QueryParam("should_escalate"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse should_escalate")
	}
	escalateEvery, err := time.ParseDuration(c.QueryParam("escalate_every"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse escalate_every")
	}
	canTimeout, err := strconv.ParseBool(c.QueryParam("can_timeout"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse can_timeout")
	}
	timeoutDuration, err := time.ParseDuration(c.QueryParam("timeout_duration"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse timeout_duration")
	}

	// Make sure the properties are valid
	if priority < 0 {
		return echo.NewHTTPError(400, "Priority must be a positive integer")
	}
	if priority >= int(queue.BucketCount) {
		return echo.NewHTTPError(400, "Priority must NOT be greater than the number of buckets")
	}
	if escalateEvery < 0 {
		return echo.NewHTTPError(400, "EscalateEvery must be a positive duration")
	}
	if timeoutDuration < 0 {
		return echo.NewHTTPError(400, "TimeoutDuration must be a positive duration")
	}

	// Get the Message
	message, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(400, "Failed to read message")
	}

	options := cschema.EnQueueOptions{
		ShouldEscalate: shouldEscalate,
		EscalationRate: escalateEvery,
		CanTimeout:     canTimeout,
		Timeout:        timeoutDuration,
	}

	// Enqueue the Message
	queue.EnQueue(message, int64(priority), options)

	return echo.NewHTTPError(200, "Message enqueued")
}

// @Summary		Serve Raw Data
// @Description	Dequeues a message from the topic with highest priority
// @Tags			Raw
// @ID				raw-serve
// @Accept			application/octet-stream
// @Produce		application/octet-stream
// @Param			name	path		string	true	"Topic Name"
// @Success		200		{string}	string	"OK"
// @Failure		400		{string}	string	"Bad Request"
// @Router			/topic/{name}/raw/dequeue [get]
// @Security		ApiKeyAuth
// @Param			Authorization	header	string	true	"Bearer {token}"
func (rt RouteHandler) RawServe(c echo.Context) error {
	// Get the Queue
	name := c.Param("name")
	if name == "" {
		return echo.NewHTTPError(400, "No topic name provided")
	}
	queue, exists := rt.Topics.Get(name)
	if !exists {
		return echo.NewHTTPError(400, "Topic not found")
	}

	// Dequeue the Message
	_, m, err := queue.DeQueue()

	if err != nil {
		return echo.NewHTTPError(400, err)
	}

	c.Response().Write(m)

	return nil
}
