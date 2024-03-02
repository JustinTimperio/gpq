package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/JustinTimperio/gpq/schema"
)

const (
	URL   = "http://localhost:4040"
	Total = 10000
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

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < Total/20; i++ {
				wg.Add(1)
				defer wg.Done()

				msg := fmt.Sprintf(`{"message":"This is message number %d"}`, i)
				req, err := http.NewRequest("POST", URL+"/topic/test/enqueue?priority=1&should_escalate=true&escalate_every=1m&can_timeout=true&timeout_duration=30m", bytes.NewBuffer([]byte(msg)))
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
			}
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < Total/10; i++ {
				req, err := http.NewRequest("GET", URL+"/topic/test/dequeue", nil)
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
			}
		}()
	}

	wg.Wait()
	fmt.Println(time.Since(timer))
}
