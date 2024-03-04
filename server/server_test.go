package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq/schema"
	"github.com/linkedin/goavro/v2"
)

const (
	URL          = "http://localhost:4040"
	Total        = 1000
	Files        = 100
	BatchTotal   = 1000000
	ItemsPerFile = 10000
	Queue        = "test"
)

func TestTransactionSpeed(t *testing.T) {

	// Login and get a token
	login, err := http.Post(
		URL+"/auth",
		"application/json",
		bytes.NewBuffer([]byte(`{"username":"admin","password":"admin"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if login.StatusCode != http.StatusOK {
		t.Fatalf("Logic expected status OK, got %v", login.StatusCode)
	}

	// Decode the token and read the key
	token := schema.Token{}
	err = json.NewDecoder(login.Body).Decode(&token)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	timer := time.Now()
	var recived int64
	var sent int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < Total/100; i++ {
				wg.Add(1)
				defer wg.Done()

				priority := fmt.Sprintf("%d", rand.Intn(10))

				msg := fmt.Sprintf(`{"message":"This is message number %d"}`, i)
				req, err := http.NewRequest("POST", URL+"/topic/"+Queue+"/enqueue?priority="+priority+"&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m", bytes.NewBuffer([]byte(msg)))
				if err != nil {
					log.Fatalln(err)
				}

				req.Header.Set("Authorization", "Bearer "+token.Token)

				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					log.Fatalln(err)
				}

				if resp.StatusCode != http.StatusOK {
					log.Fatalf("Expected status OK, got %v", resp.StatusCode)
				}

				resp.Body.Close()
				atomic.AddInt64(&sent, 1)
			}
		}()
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < Total/100; i++ {
				req, err := http.NewRequest("GET", URL+"/topic/"+Queue+"/dequeue", nil)
				if err != nil {
					log.Fatalln(err)
				}

				req.Header.Set("Authorization", "Bearer "+token.Token)

				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					log.Fatalln(err)
				}

				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					i--
					continue
				}
				atomic.AddInt64(&recived, 1)
			}
		}()
	}

	wg.Wait()
	fmt.Println("Total time", time.Since(timer), "Sent", atomic.LoadInt64(&sent), "Recived", atomic.LoadInt64(&recived))
}

func TestTransactionSpeedAvro(t *testing.T) {
	// Login and get a token
	login, err := http.Post(
		URL+"/auth",
		"application/json",
		bytes.NewBuffer([]byte(`{"username":"admin","password":"admin"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if login.StatusCode != http.StatusOK {
		t.Fatalf("Logic expected status OK, got %v", login.StatusCode)
	}

	// Decode the token and read the key
	token := schema.Token{}
	err = json.NewDecoder(login.Body).Decode(&token)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	timer := time.Now()
	var sent int64
	var recived int64
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < Files/10; i++ {
				wg.Add(1)
				defer wg.Done()

				priority := fmt.Sprintf("%d", rand.Intn(10))
				s, body := RandomAvroFile()

				req, err := http.NewRequest("POST", URL+"/topic/"+Queue+"/avro/enqueue?priority="+priority+"&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m", bytes.NewBuffer(body))
				if err != nil {
					log.Fatalln(err)
				}

				req.Header.Set("Authorization", "Bearer "+token.Token)

				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					log.Fatalln(err)
				}

				if resp.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(resp.Body)
					log.Fatalf("Expected status OK, got %v Body: %v", resp.StatusCode, string(body))
				}
				atomic.AddInt64(&sent, int64(s))
			}
		}()

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for BatchTotal > int(atomic.LoadInt64(&recived)) {
					req, err := http.NewRequest("GET", URL+"/topic/"+Queue+"/avro/dequeue?records=5000", nil)
					if err != nil {
						log.Fatalln(err)
					}

					req.Header.Set("Authorization", "Bearer "+token.Token)

					client := &http.Client{}
					resp, err := client.Do(req)
					if err != nil {
						log.Fatalln(err)
					}

					if resp.StatusCode != http.StatusOK {
						b, _ := io.ReadAll(resp.Body)
						fmt.Printf("Expected status OK, got %v Response: %v", resp.StatusCode, string(b))
						time.Sleep(100 * time.Millisecond)
						continue
					}
					// Reading OCF data
					ocfReader, err := goavro.NewOCFReader(resp.Body)
					if err != nil {
						log.Fatalln("Reader Create Error:", err)
					}

					for ocfReader.Scan() {
						_, err := ocfReader.Read()
						if err != nil {
							log.Fatalln(err)
						}
						atomic.AddInt64(&recived, 1)
					}

				}
			}()
		}
	}
	wg.Wait()
	fmt.Println("Total time", time.Since(timer), "Sent", atomic.LoadInt64(&sent), "Recived", atomic.LoadInt64(&recived))
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
