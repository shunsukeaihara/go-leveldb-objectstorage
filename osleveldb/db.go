package osleveldb

import (
	"math/rand"
	"time"
)

type cachedItem struct {
	val     interface{}
	ttl     time.Duration
	created time.Time
	ok      bool
}

func (i *cachedItem) hasExpired() bool {
	return time.Since(i.created) > i.ttl
}

type dbResult struct {
	val interface{}
	ok  bool
	hit bool
}

type UnmarshalFunc func([]byte) (interface{}, error)
type MarshalFunc func(interface{}) ([]byte, error)

type dbGetCmd struct {
	key    string
	fun    UnmarshalFunc
	result chan *dbResult
	ttl    time.Duration
}

func NewDBGetCmd(key string, fun UnmarshalFunc, ttl int) *dbGetCmd {
	// ttl -> second
	return &dbGetCmd{
		key,
		fun,
		make(chan *dbResult),
		time.Duration(ttl+int(rand.Int63n(int64(ttl/10+1)))) * time.Second,
	}
}
