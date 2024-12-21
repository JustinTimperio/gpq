package gpq_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/JustinTimperio/gpq/v1/schema"
	"github.com/dgraph-io/badger/v4"
)

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func numberOfItemsInDB(path string) int {
	var total int
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return 0
	}

	db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {

			val := it.Item()
			val.Value(func(v []byte) error {
				var buf bytes.Buffer
				buf.Write(v)
				obj := schema.Item[int]{}
				err = gob.NewDecoder(&buf).Decode(&obj)
				if err != nil {
					return err
				}
				jsonObj, err := json.MarshalIndent(obj, "", "  ")
				if err != nil {
					return err
				}
				fmt.Println(string(jsonObj))
				total++
				return nil

			})
		}

		return nil
	})

	return total
}
