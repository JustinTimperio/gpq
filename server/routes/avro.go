package routes

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"time"

	"github.com/JustinTimperio/gpq/schema"
	"github.com/labstack/echo/v4"
	"github.com/linkedin/goavro/v2"
)

func (rt RouteHandler) AvroReceive(c echo.Context) error {
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

	reader, err := goavro.NewOCFReader(c.Request().Body)
	if err != nil {
		rt.Logger.Errorw("Failed to create Avro reader", "error", err)
		return echo.NewHTTPError(400, "Failed to create Avro reader")
	}

	var batchErrors []error
	var good int
	for reader.Scan() {

		msg, err := reader.Read()
		if err != nil {
			batchErrors = append(batchErrors, err)
			continue
		}

		record := schema.AvroDataEntry{
			Data:   []interface{}{msg},
			Schema: reader.Codec().Schema(),
		}

		// Wrap the record in a buffer
		var wrapper bytes.Buffer
		wenc := gob.NewEncoder(&wrapper)
		err = wenc.Encode(record)
		if err != nil {
			fmt.Println(err)
			return echo.NewHTTPError(400, "Failed to encode message")
		}

		queue.EnQueue(wrapper.Bytes(), int64(priority), shouldEscalate, escalateEvery, canTimeout, timeoutDuration)
		good++
	}

	if len(batchErrors) > 0 {
		fmt.Println(batchErrors)
		return echo.NewHTTPError(400, fmt.Sprintf("Received batch with errors! Received: %d Failed: %d", good, len(batchErrors)))
	}

	return nil
}

// AvroServe serves Avro data from the queue in batches
// In the future this should have some mechanism to not drop messages during errors
// This is a pretty hard problem to solve, but it should be solved for mission critical applications
// Right now, if there is an error, the message is dropped during a batch process
func (rt RouteHandler) AvroServe(c echo.Context) error {
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

	var attempts int
	var collected int
	var writer *goavro.OCFWriter
	var lastSchema string
	var fileBuf bytes.Buffer

	for collected < recordCount {

		// Todo: Make not a magic number
		if attempts > 10 {
			if collected == 0 {
				return echo.NewHTTPError(400, "Failed to collect any messages")
			}

			c.Response().Write(fileBuf.Bytes())
			return nil
		}

		// Dequeue the Message
		_, msg, err := queue.DeQueue()
		if err != nil {
			attempts++
			// TODO: Make not a magic number
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Decode the message
		var record schema.AvroDataEntry
		var buf bytes.Buffer
		buf.Write(msg)
		err = gob.NewDecoder(&buf).Decode(&record)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to decode message")
		}
		if record.Schema == "" {
			return echo.NewHTTPError(400, "Failed to find schema in message")
		}

		// On the first iteration, create the writer
		if collected == 0 {

			lastSchema = record.Schema
			writer, err = goavro.NewOCFWriter(goavro.OCFConfig{
				W:      &fileBuf,
				Schema: record.Schema,
			})
			if err != nil {
				return echo.NewHTTPError(400, "Failed to create Avro writer")
			}
		}
		if record.Schema != lastSchema {
			return echo.NewHTTPError(200, "Schema mismatch in batch, one message was dropped!")
		}

		err = writer.Append(record.Data)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to append message")
		}
		collected++
	}

	c.Response().Write(fileBuf.Bytes())
	return nil
}
