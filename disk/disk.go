package disk

import (
	"errors"

	"github.com/JustinTimperio/gpq/schema"

	"github.com/dgraph-io/badger/v4"
	bOptions "github.com/dgraph-io/badger/v4/options"
)

type Disk[T any] struct {
	diskCache *badger.DB
}

func NewDiskCache[T any](bLogger badger.Logger, options schema.GPQOptions) (*Disk[T], error) {

	if options.DiskCachePath == "" {
		return nil, errors.New("Error creating disk cache: path is empty")
	}

	opts := badger.DefaultOptions(options.DiskCachePath)
	opts.Logger = bLogger
	if options.DiskCacheCompression {
		opts.Compression = bOptions.ZSTD
	}
	if options.DiskEncryptionEnabled {
		opts.WithEncryptionKey(options.DiskEncryptionKey)
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.New("Error opening disk cache: " + err.Error())
	}

	return &Disk[T]{diskCache: db}, nil
}

func (d *Disk[T]) Close() error {
	d.diskCache.Sync()
	return d.diskCache.Close()
}

func (d *Disk[T]) ItemsInDB() uint {
	var count uint
	_ = d.diskCache.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}

		return nil
	})
	return count
}

func (d *Disk[T]) ProcessBatch(batch []*schema.Item[T]) error {
	txn := d.diskCache.NewTransaction(true)
	defer txn.Discard()

	for i := 0; i < len(batch); i++ {
		entry := batch[i]
		b, err := entry.ToBytes()
		if err != nil {
			return err
		}
		err = txn.Set(entry.DiskUUID, b)
		if err == badger.ErrTxnTooBig {
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = d.diskCache.NewTransaction(true)
			txn.Set(entry.DiskUUID, b)

		} else if err != nil {
			return err
		}
	}

	// Commit the final transaction, if it has any pending writes
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (d *Disk[T]) DeleteBatch(batch []*schema.Item[T]) error {
	txn := d.diskCache.NewTransaction(true)
	defer txn.Discard()

	for i := 0; i < len(batch); i++ {
		entry := batch[i]
		err := txn.Delete(entry.DiskUUID)
		if err == badger.ErrTxnTooBig {
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = d.diskCache.NewTransaction(true)
			txn.Delete(entry.DiskUUID)
		} else if err != nil {
			return err
		}
	}

	// Commit the final transaction, if it has any pending writes
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (d *Disk[T]) WriteSingle(key []byte, value schema.Item[T]) error {
	b, err := value.ToBytes()
	if err != nil {
		return err
	}
	err = d.diskCache.Update(func(txn *badger.Txn) error {
		return txn.Set(key, b)
	})

	return err
}

func (d *Disk[T]) DeleteSingle(key []byte) error {
	err := d.diskCache.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	return err
}

func (d *Disk[T]) RestoreFromDisk() ([]*schema.Item[T], error) {
	var items []*schema.Item[T]

	// Re-add items to the GPQ from the disk cache
	err := d.diskCache.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			var value []byte
			item := it.Item()
			key := item.Key()

			// Get the item from the disk cache
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			item.Value(func(val []byte) error {
				value = append([]byte{}, val...)
				return nil
			})

			if len(value) == 0 {
				return errors.New("Error reading from disk cache: value is empty")
			}

			obj := new(schema.Item[T])
			obj.FromBytes(value)
			obj.WasRestored = true
			items = append(items, obj)

		}

		return nil
	})
	if err != nil {
		return nil, errors.New("Error reading from disk cache: " + err.Error())
	}

	return items, err
}
