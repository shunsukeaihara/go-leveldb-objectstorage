package osleveldb

import (
	"math/rand"
	"time"
)

type cachedItem struct {
	val     interface{}
	ttl     int64
	created int64
	ok      bool
}

func (i *cachedItem) hasExpired() bool {
	now := time.Now().Unix()
	if now-i.created > i.ttl {
		return true
	} else {
		return false
	}
}

type dbResult struct {
	val interface{}
	ok  bool
	hit bool
}

type unmarshalFunc func([]byte) (interface{}, error)
type marshalFunc func(interface{}) ([]byte, error)

type dbGetCmd struct {
	key    string
	fun    unmarshalFunc
	result chan *dbResult
	ttl    int64 // sec -> ToDO: change to time.Duration
}

func NewDBGetCmd(key string, fun unmarshalFunc, ttl int64) *dbGetCmd {
	return &dbGetCmd{
		key,
		fun,
		make(chan *dbResult),
		ttl + rand.Int63n(int64(ttl/10+1)),
	}
}
