package keystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {

	db, err := New("test.bolt")
	assert.Nil(t, err)

	s := "hello, world"
	err = db.Set("test1", "key1", s)
	assert.Nil(t, err)

	var s2 string
	err = db.Get("test1", "key1", &s2)
	assert.Nil(t, err)
	assert.Equal(t, s, s2)

	err = db.Get("test1", "nokey", &s2)
	assert.Equal(t, KeyErrNotExist, err)
	err = db.Get("nobucket", "nokey", &s2)
	assert.Equal(t, BucketErrNotExist, err)
}
