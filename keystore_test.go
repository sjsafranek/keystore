package keystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {
	s := "hello, world"
	err := Set("test1", "key1", s)
	assert.Nil(t, err)

	var s2 string
	err = Get("test1", "key1", &s2)
	assert.Nil(t, err)
	assert.Equal(t, s, s2)

	err = Get("test1", "nokey", &s2)
	assert.Equal(t, ErrNotExist, err)
	err = Get("nobucket", "nokey", &s2)
	assert.Equal(t, ErrNotExist, err)
}
