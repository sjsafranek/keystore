package keystore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/schollz/golock"
	bolt "go.etcd.io/bbolt"
)

var BucketErrNotExist = errors.New("Bucket does not exist")
var KeyErrNotExist = errors.New("Key does not exist")

type KeyStore struct {
	db    *bolt.DB
	glock *golock.Lock
}

func New(filename string) (*KeyStore, error) {

	// first initiate lockfile
	lock_file := strings.Replace(filename, ".db", ".lock", -1)
	glock := golock.New(
		golock.OptionSetName(lock_file),
		golock.OptionSetInterval(1*time.Millisecond),
		golock.OptionSetTimeout(60*time.Second),
	)

	err := glock.Lock()
	if err != nil {
		return &KeyStore{}, err
	}
	//.end

	// db, err := bolt.Open(filename, 0666, nil)
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 1 * time.Second})
	return &KeyStore{db: db, glock: glock}, err
}

// Close database connection and remove file lock
func (self *KeyStore) Close() error {
	self.db.Close()
	return self.glock.Unlock()
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
			return BucketErrNotExist
		}
		v = b.Get([]byte(key))
		if v == nil {
			return KeyErrNotExist
		}
		return nil
	})
	if err != nil {
		return
	}
	err = json.Unmarshal(v, &value)
	return
}
