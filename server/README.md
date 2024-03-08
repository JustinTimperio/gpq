# GPQ Server

GPQ provides a fully featured server that while fairly simple, provides strong performance and reliability in critical workloads. It is currently under heavy development so change can and will be expected in not only the API but also the features that are available to it.

## Table of Contents
- [GPQ Server](#gpq-server)
  - [Table of Contents](#table-of-contents)
  - [Setup and Settings](#setup-and-settings)
  - [API Documentation with Swagger](#api-documentation-with-swagger)
  - [Tokens and Users](#tokens-and-users)
  - [Adding and Removing Topics](#adding-and-removing-topics)
  - [Batching with Avro](#batching-with-avro)
    - [Example of Batching with Avro](#example-of-batching-with-avro)
  - [Batching with Arrow](#batching-with-arrow)
    - [Example of Batching with Arrow](#example-of-batching-with-arrow)
  - [Single Message Usage](#single-message-usage)

## Setup and Settings
Setting up the server is a simple process that only requires creating a directory with permissions to `/opt/gpq` for the server to store its settings and finally place the `gpq-server` binary somewhere in your path. The server will automatically create the necessary directories and files on startup if they do not exist. 

Configuring the sever can be done multiple ways through a yaml file or through environment variables. The server will look for a file called `config.yaml` in the `/opt/gpq` directory and use it if it exists. The config can be over written by setting environment variables. The following is a list of the available settings:
- `gpq_server_port` - The port the server will listen on. Default is `4040`.
- `gpq_server_host` - The host the server will listen on. Default is `localhost`
- `gpq_settings_path` - The path to the settings directory. Default is `/opt/gpq`.
- `gpq_log_path` - The path to the log directory. Default is `/opt/gpq/gpq.log`.
- `gpq_stdout` - If set to `true` the server will log to stdout as well as to the disk. Default is `false`.
- `gpq_auth_topics` - If suuet to `true` the server will require a token to access topics. Default is `true`.
- `gpq_auth_settings` - If set to `true` the server will require a token to access settings. Default is `true`.
- `gpq_auth_management` - If set to `true` the server will require a token to access management functions. Default is `true`.
- `gpq_admin_user` - The username for the admin user. Default is `admin`.
- `gpq_admin_pass` - The password for the admin user. Default is `admin`.

## API Documentation with Swagger
All of the server's API endpoints are documented with Swagger and can be accessed by navigating to the server's root URL. For example, if the server is running on `localhost:4040` then the Swagger documentation can be accessed by navigating to `http://localhost:4040/swagger/index.html`. The Swagger documentation provides a simple way to interact with the server and test its functionality.

## Tokens and Users 
Users must first be created by the admin user with a POST request to the `/users/add` endpoint. The request must include a JSON body with the following fields:
- `username` - The username for the new user.
- `password` - The password for the new user.

Once a user is created they can request to the `/auth` endpoint with a POST request to receive a token. The request must include a JSON body with the following fields:
- `username` - The username for the user.
- `password` - The password for the user.

The server will respond with a JSON body that includes the token. The token must be included in the headers of all requests to the server. The token must be included in the headers with the key `Authorization` and the value `Bearer <token>`. For example, if the token is `1234` then the header would be `Authorization: Bearer 1234`. The token is good for 24 hours and will automatically be removed when it is no longer valid.

## Adding and Removing Topics
Adding topics and removing them currently requires a token and currently does not support complex permissions yet. The request is a simple parametrized POST request to the `/topics/add` and `/topics/remove` endpoints.
The url should look something like:
```
http://localhost:4040/management/topic/add?name=namehere&disk_path=/opt/gpq/topic/namehere&buckets=10&sync_to_disk=true&reprioritize=true&reprioritize_rate=1m&lazy_disk_sync=true&batch_size=1000"
```
     
The token should be included in the headers. The server will respond with a JSON body that includes the status of the request.

## Batching with Avro
Avro is a binary serialization format that is used to batch messages together which makes it a great choice batched workloads. You can batch messages with Avro by sending a POST request to the `/topic/{name}/avro/enqueue` or `/topic/{name}/avro/dequeue` endpoints. The request must contain a Token in the headers and a avro binary in the body. The server will respond with a JSON body that includes the status of the request.

### Example of Batching with Avro
```go
func main() {
    Token := "YourToken"
	s, body := RandomAvroFile()

	req, err := http.NewRequest("POST", "http:localhost:4040/topic/name/avro/enqueue?priority=1&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m", bytes.NewBuffer(body))
	
    if err != nil {
		log.Fatalln(err)
	}

	req.Header.Set("Authorization", "Bearer "+Token)

    client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Expected status OK, got %v Body: %v", resp.StatusCode, string(body))
	}
}

func RandomAvroFile() (sent int, b []byte) {
	avroSchema := `
	{
	  "type": "record",
	  "name": "test_schema",
	  "fields": [
		{
		  "name": "time",
		  "type": "long"
		},
		{
		  "name": "customer",
		  "type": "string"
		}
	  ]
	}`

	// Writing OCF data
	var ocfFileContents bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &ocfFileContents,
		Schema: avroSchema,
	})
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < ItemsPerFile; i++ {
		err = writer.Append([]map[string]interface{}{
			{
				"time":     time.Now().UnixNano(),
				"customer": "customer-" + fmt.Sprintf("%d", i),
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		sent++
	}

	return sent, ocfFileContents.Bytes()
}
```


## Batching with Arrow
Arrow is a in memory format that can be used to batch messages together. You can batch messages with Arrow by sending a POST request to the `/topic/{name}/arrow/enqueue` or `/topic/{name}/arrow/dequeue` endpoints. The request must contain a Token in the headers and a arrow binary in the body. The server will respond with a JSON body that includes the status of the request.

### Example of Batching with Arrow
```go
func main() {
    Token := "YourToken"
	s, body := RandomArrowFile()

	req, err := http.NewRequest("POST", "http:localhost:4040/topic/name/arrow/enqueue?priority=1&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m", bytes.NewBuffer(body))
	
    if err != nil {
		log.Fatalln(err)
	}

	req.Header.Set("Authorization", "Bearer "+Token)

    client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Expected status OK, got %v Body: %v", resp.StatusCode, string(body))
	}
}

func RandomArrowFile() (sent int, b []byte) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Create a buffer to write the Arrow data to
	ws := &ws.WriterSeeker{}

	// Create a new Arrow file writer
	w, err := ipc.NewFileWriter(ws, ipc.WithSchema(schema))
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < ItemsPerFile; i++ {
		builder.Field(0).(*array.Int32Builder).AppendValues([]int32{rand.Int31()}, nil)
		builder.Field(1).(*array.Float64Builder).AppendValues([]float64{rand.ExpFloat64()}, nil)

		rec := builder.NewRecord()
		defer rec.Release()

		// Write the record to the file
		if err := w.Write(rec); err != nil {
			log.Fatal(err)
		}
		sent++
	}

	// Close the writer
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Return the bytes of the Arrow file
	ws.Close()
	return sent, ws.Buf.Bytes()
}
```

## Single Message Usage
Messages can also be sent to the server one at a time. You can send a single message by sending a POST request to the `/topic/{name}/raw/enqueue` or `/topic/{name}/raw/dequeue` endpoints. The request must contain a Token in the headers and a JSON body with the message. The server will respond with a JSON body that includes the status of the request.

```sh
curl -X POST 'http://localhost:4040/topic/ds/raw/enqueue?priority=1&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m' -H 'Content-Type: text/plain' -H "Accept: application/json" -H "Authorization: Bearer $TOKEN" -d 'Hello World!'
``