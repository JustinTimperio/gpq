package routes

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/JustinTimperio/gpq/server/schema"
	"github.com/labstack/echo/v4"
	"github.com/parquet-go/parquet-go"
)

func (rt RouteHandler) ParquetReceive(c echo.Context) error {
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
		return echo.NewHTTPError(400, "Failed to read parquet file")
	}

	fr := bytes.NewReader(b)
	pf := parquet.NewReader(fr)
	defer pf.Close()

	p, err := parquet.OpenFile(fr, int64(len(b)))
	if err != nil {
		return echo.NewHTTPError(400, "Failed to open parquet file")
	}
	fileSchema := p.Schema()

	structFields := []schema.ParquetSchema{}
	fields := fileSchema.Fields()
	for _, field := range fields {

		tag := fmt.Sprintf(`parquet:"%s,%t,%s,%t"`, field.Name(), field.Optional(), field.Encoding(), field.Required())
		t := field.GoType()

		structFields = append(structFields, schema.ParquetSchema{
			Name: field.Name(),
			Type: t,
			Tag:  tag,
		})
	}

	// Read the parquet file
	for {
		var entry interface{}
		err := pf.Read(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return echo.NewHTTPError(400, "Failed to read parquet file")
		}

		pde := schema.ParquetDataEntry{
			Data:   entry,
			Schema: structFields,
		}

		// Read the message into a gob object
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(pde)
		if err != nil {
			fmt.Println("Failed to encode message", err)
			return echo.NewHTTPError(400, "Failed to encode message"+err.Error())
		}

		// Add the record to the queue
		queue.EnQueue(buf.Bytes(), int64(priority), shouldEscalate, escalateEvery, canTimeout, timeoutDuration)
	}

	return nil
}

func (rt RouteHandler) ParquetServe(c echo.Context) error {
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
	var fileSchema *parquet.Schema
	var buf = new(bytes.Buffer)

	// TOO: Make not a magic number
	for {

		if attempts > 10 {
			return echo.NewHTTPError(400, "Failed to collect any messages")
		}

		peek, err := queue.Peek()
		if err != nil {
			attempts++
			time.Sleep(10 * time.Millisecond)
			continue
		}

		entry := schema.ParquetDataEntry{}
		var buf bytes.Buffer
		buf.Write(peek)
		err = gob.NewDecoder(&buf).Decode(&entry)
		if err != nil {
			fmt.Println("Failed to decode message", err)
			return echo.NewHTTPError(400, "Failed to decode message"+err.Error())
		}

		structFields := []reflect.StructField{}

		for i := 0; i < len(entry.Schema); i++ {
			structFields = append(structFields, reflect.StructField{
				Name: entry.Schema[i].Name,
				Type: entry.Schema[i].Type,
				Tag:  reflect.StructTag(entry.Schema[i].Tag),
			})
		}
		structType := reflect.StructOf(structFields)
		structElem := reflect.New(structType)

		fileSchema = parquet.NewSchema("GPQ", parquet.SchemaOf(structElem.Interface()))
		break
	}

	wr := parquet.NewWriter(buf, fileSchema, parquet.MaxRowsPerRowGroup(1000))
	for collected < recordCount {

		// Todo: Make not a magic number
		if attempts > 10 {
			if collected == 0 {
				return echo.NewHTTPError(400, "Failed to collect any messages")
			}

			wr.Close()
			c.Response().Write(buf.Bytes())
			return nil
		}

		// Dequeue the Message
		_, msg, err := queue.DeQueue()
		if err != nil {
			attempts++
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Set the schema on the first iteration
		if collected == 0 {

		}
		// Decode the message
		entry := schema.ParquetDataEntry{}
		var buf bytes.Buffer
		buf.Write(msg)
		err = gob.NewDecoder(&buf).Decode(&entry)
		if err != nil {
			fmt.Println("Failed to decode message", err)
			return echo.NewHTTPError(400, "Failed to decode message"+err.Error())
		}

		err = wr.Write(entry.Data)
		if err != nil {
			return echo.NewHTTPError(400, "Failed to write message"+err.Error())
		}

		collected++
	}

	// Set the response
	wr.Close()
	c.Response().Write(buf.Bytes())
	return nil
}
