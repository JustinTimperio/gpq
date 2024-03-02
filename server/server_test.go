package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq/schema"
)

const (
	URL   = "http://localhost:4040"
	Total = 100000
	Queue = "ds"
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
