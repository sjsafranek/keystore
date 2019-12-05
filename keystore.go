package keystore

import (
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// ErrNotExist returned when there is no key
var ErrNotExist = errors.New("does not exist")

type KeyStore struct {
	db *bolt.DB
}

func New(filename string) (*KeyStore, error) {
	db, err := bolt.Open(filename, 0666, nil)
	return &KeyStore{db: db}, err
}

// Set will Marshal the value and insert it into the bucket with specified key
func (self *KeyStore) Set(bucket string, key string, value interface{}) (err error) {
	valueB, err := json.Marshal(value)
	if err != nil {
		return
	}

	err = self.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return b.Put([]byte(key), valueB)
	})
	return
}

// Get will return interface from specified bucket with specified key.
// returns error if there is no key
func (self *KeyStore) Get(bucket string, key string, value interface{}) (err error) {
	var v []byte
	err = self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return ErrNotExist
		}
		v = b.Get([]byte(key))
		if v == nil {
			return ErrNotExist
		}
		return nil
	})
	if err != nil {
		return
	}
	err = json.Unmarshal(v, &value)
	return
}
