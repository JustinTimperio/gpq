package routes

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/labstack/echo/v4"
)

func (rt RouteHandler) ArrowReceive(c echo.Context) error {
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

	// initialize the reader with the stream of data
	rdr, err := ipc.NewReader(c.Request().Body)
	if err != nil {
		rt.Logger.Errorw("Failed to create reader", "error", err)
	}
	defer rdr.Release()

	for rdr.Next() {
		msg, err := rdr.Read()
		if err != nil {
			rt.Logger.Errorw("Failed to read message", "error", err)
		}

		// Encode the item
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(msg)
		if err != nil {
			return err
		}
		blob := buf.Bytes()

		queue.EnQueue(blob, int64(priority), shouldEscalate, escalateEvery, canTimeout, timeoutDuration)
		msg.Release()
	}

	return c.String(200, "Message batch enqueued")
}

func (rt RouteHandler) ArrowServe(c echo.Context) error {
	// Get the Queue
	name := c.Param("name")
	if name == "" {
		return echo.NewHTTPError(400, "No topic name provided")
	}
	recordCount, err := strconv.Atoi(c.QueryParam("records"))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to parse record_count")
	}

	queue, exists := rt.Topics.Get(name)
	if !exists {
		return echo.NewHTTPError(400, "Topic not found")
	}

	var wr *ipc.Writer
	for i := 0; i < recordCount; i++ {
		// Dequeue the Message
		_, msg, err := queue.DeQueue()
		if err != nil {
			return echo.NewHTTPError(400, "Failed to dequeue message")
		}

		// Decode the message
		var message []array.Record
		var buf bytes.Buffer
		buf.Write(msg)
		err = gob.NewDecoder(&buf).Decode(&message)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to decode message")
		}

		// Record Reader
		rr, err := array.NewRecordReader(message[0].Schema(), message)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to create record reader")
		}
		defer rr.Release()

		// Set the schema on the first iteration
		if i == 0 {
			wr = ipc.NewWriter(c.Response(), ipc.WithSchema(rr.Schema()))
			defer wr.Close()
		}

		// Write the record
		err = wr.Write(rr.Record())
		if err != nil {
			return echo.NewHTTPError(400, "Failed to write message")
		}
	}

	// Set the response
	c.Response().Header().Set(echo.HeaderContentType, "application/vnd.apache.arrow.stream")

	return nil
}
