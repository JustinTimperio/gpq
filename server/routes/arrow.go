package routes

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/JustinTimperio/gpq/server/schema"
	"github.com/JustinTimperio/gpq/server/ws"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/arrio"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/labstack/echo/v4"
)

// @Summary		Receive Arrow Data
// @Description	Receives Arrow data and enqueues it into a topic
// @Tags			Arrow
// @ID				arrow-receive
// @Accept			application/vnd.apache.arrow
// @Produce		json
// @Param			name				path		string	true	"Topic Name"
// @Param			priority			query		int		true	"Priority"
// @Param			should_escalate		query		bool	true	"Should Escalate"
// @Param			escalate_every		query		string	true	"Escalate Every"
// @Param			can_timeout			query		bool	true	"Can Timeout"
// @Param			timeout_duration	query		string	true	"Timeout Duration"
// @Success		200					{string}	string	"OK"
// @Failure		400					{string}	string	"Bad Request"
// @Router			/arrow/receive/{name} [post]
// @Security		ApiKeyAuth
// @Param			Authorization	header	string	true	"Bearer {token}"
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

	b, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(400, "Failed to read body: "+err.Error())
	}
	rdr, err := ipc.NewFileReader(bytes.NewReader(b))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to create reader: "+err.Error())
	}
	defer rdr.Close()

	for i := 0; i < rdr.NumRecords(); i++ {
		msg, err := rdr.Record(i)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to read message"+err.Error())
		}
		defer msg.Release()

		buff := bytes.NewBuffer(nil)
		ww := ipc.NewWriter(buff, ipc.WithSchema(rdr.Schema()))
		defer ww.Close()

		err = ww.Write(msg)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to write message"+err.Error())
		}

		var fields []schema.ArrowSchema
		for _, field := range rdr.Schema().Fields() {
			fields = append(fields, schema.ArrowSchema{
				Name: field.Name,
				Type: field.Type,
			})
		}

		entry := schema.ArrowDataEntry{
			Meta:   rdr.Schema().Metadata().ToMap(),
			Data:   buff.Bytes(),
			Schema: fields,
		}

		// Read the message into a gob object
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(entry)
		if err != nil {
			fmt.Println("Failed to encode message", err)
			return echo.NewHTTPError(400, "Failed to encode message"+err.Error())
		}

		queue.EnQueue(buf.Bytes(), int64(priority), shouldEscalate, escalateEvery, canTimeout, timeoutDuration)
	}

	return nil
}

// @Summary		Serve Arrow Data
// @Description	Batches and serves Arrow data from a topic
// @Tags			Arrow
// @ID				arrow-serve
// @Produce		application/vnd.apache.arrow.stream
// @Param			name	path		string	true	"Topic Name"
// @Param			records	query		int		false	"Number of records to collect"
// @Success		200		{array}		byte	"Arrow Data"
// @Failure		400		{string}	string	"Bad Request"
// @Router			/arrow/serve/{name} [get]
// @Security		ApiKeyAuth
// @Param			Authorization	header	string	true	"Bearer {token}"
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

	var wr *ipc.FileWriter
	var attempts int
	var collected int
	w := &ws.WriterSeeker{}
	c.Response().Header().Set(echo.HeaderContentType, "application/vnd.apache.arrow.stream")

	for collected < recordCount {

		// Todo: Make not a magic number
		if attempts > 10 {
			if collected == 0 {
				return echo.NewHTTPError(400, "Failed to collect any messages")
			}

			wr.Close()
			c.Response().Write(w.Buf.Bytes())
			return nil
		}

		// Dequeue the Message
		_, msg, err := queue.DeQueue()
		if err != nil {
			attempts++
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Decode the message
		entry := schema.ArrowDataEntry{}
		var buf bytes.Buffer
		buf.Write(msg)
		err = gob.NewDecoder(&buf).Decode(&entry)
		if err != nil {
			fmt.Println("Failed to decode message", err)
			return echo.NewHTTPError(400, "Failed to decode message"+err.Error())
		}

		// Set the schema on the first iteration
		if collected == 0 {
			meta := arrow.MetadataFrom(entry.Meta)
			var fields []arrow.Field
			for _, field := range entry.Schema {
				data := arrow.Field{
					Name: field.Name,
					Type: field.Type,
				}
				fields = append(fields, data)
			}

			wr, err = ipc.NewFileWriter(w, ipc.WithSchema(arrow.NewSchema(fields, &meta)))
			if err != nil {
				return echo.NewHTTPError(400, "Failed to create writer")
			}
			defer wr.Close()
		}

		// Record Reader
		rr, err := ipc.NewReader(bytes.NewReader(entry.Data))
		if err != nil {
			return echo.NewHTTPError(400, "Failed to create record reader")
		}
		defer rr.Release()

		_, err = arrio.Copy(wr, rr)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to copy record reader to writer"+err.Error())
		}
		collected++
	}

	// Set the response
	wr.Close()
	c.Response().Write(w.Buf.Bytes())
	return nil
}
