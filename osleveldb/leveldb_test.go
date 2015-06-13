package osleveldb

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/GoogleCloudPlatform/kubernetes/Godeps/_workspace/src/code.google.com/p/go-uuid/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	testdbpath = "/tmp/ldbtest/"
)

type testStruct struct {
	Title string `msgpack:"title"`
	URL   string `msgpack:"url"`
	Score int    `msgpack:"score"`
}

func makeData() []byte {
	data := make([]*testStruct, 0, 100)
	for i := 1; i <= 100; i++ {
		data = append(data, &testStruct{
			"title", "url", i})
	}
	bytes, _ := msgpack.Marshal(data)
	return bytes
}

func UnmarshalTestStruct(bytes []byte) (interface{}, error) {
	var data *[]testStruct
	if err := msgpack.Unmarshal(bytes, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func makeDB(dbpath string, num int) {
	db, _ := leveldb.OpenFile(dbpath, nil)
	defer func() {
		db.Close()
	}()
	for i := 1; i <= num; i++ {
		key := fmt.Sprintf("testdata:%d", i)
		db.Put([]byte(key), makeData(), nil)
	}
}

func BenchmarkRead10000Data(b *testing.B) {
	os.MkdirAll(testdbpath, 0700)
	dirname := uuid.NewUUID().String()
	dbpath := path.Join(testdbpath, dirname)

	makeDB(dbpath, 10000)
	db, _ := leveldb.OpenFile(dbpath, nil)

	conf := LevelDBConf{"test", testdbpath, StorageS3{"", "", ""}, nil}

	ldb := NewLevelDBWithDB(db, dbpath, conf)
	go ldb.run()
	defer func() {
		ldb.Exit()
	}()
	b.ResetTimer()

	for i := 1; i <= 10000; i++ {
		key := fmt.Sprintf("testdata:%d", i)
		ldb.Get(key, UnmarshalTestStruct)
	}
}

func BenchmarkRead10000CachedData(b *testing.B) {
	os.MkdirAll(testdbpath, 0700)
	dirname := uuid.NewUUID().String()
	dbpath := path.Join(testdbpath, dirname)

	makeDB(dbpath, 10000)
	db, _ := leveldb.OpenFile(dbpath, nil)

	conf := LevelDBConf{"test", testdbpath, nil, nil}

	ldb := NewLevelDBWithDB(db, dbpath, conf)
	go ldb.run()
	defer func() {
		ldb.Exit()
	}()

	for i := 1; i <= 10000; i++ {
		key := fmt.Sprintf("testdata:%d", i)
		ldb.Get(key, UnmarshalTestStruct)
	}

	b.ResetTimer()

	for i := 1; i <= 10000; i++ {
		key := fmt.Sprintf("testdata:%d", i)
		ldb.Get(key, UnmarshalTestStruct)
	}
}

func TestDownload(t *testing.T) {

}
