package osleveldb

import (
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
)

//LevelDB wraps leveldb with cache and automaticaly update from object storage(such as S3).
type LevelDB struct {
	get         chan *dbGetCmd
	switching   chan *switchDB
	close       chan chan struct{}
	reset       chan struct{}
	expiration  chan struct{}
	db          *leveldb.DB // LevelDB
	dbpath      string
	cache       map[string]*cachedItem
	downloading bool
	mu          sync.RWMutex
	dbConf      LevelDBConf
}

type switchDB struct {
	db     *leveldb.DB
	dbpath string
}

// NewLevelDBWithDB makes struct LevelDB with leveldb object and leveldb path.
func NewLevelDBWithDB(db *leveldb.DB, dbpath string, dbConf LevelDBConf) *LevelDB {
	if dbConf.Options == nil {
		// set defaut value
		dbConf.Options = &defaultOption
	}
	ldb := LevelDB{
		make(chan *dbGetCmd, 999),
		make(chan *switchDB),
		make(chan chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
		db,
		dbpath,
		map[string]*cachedItem{},
		false,
		sync.RWMutex{},
		dbConf,
	}
	return &ldb
}

// NewLevelDB makes struct LevelDB and download leveldb file from object storage.
func NewLevelDB(dbConf LevelDBConf) *LevelDB {
	ldb := NewLevelDBWithDB(nil, "", dbConf)
	newdb := ldb.download(false)
	ldb = NewLevelDBWithDB(newdb.db, newdb.dbpath, dbConf)
	go ldb.run()
	return ldb
}

func (ldb *LevelDB) getFromLdb(cmd *dbGetCmd) *cachedItem {
	key := []byte(cmd.key)
	bytes, err := ldb.db.Get(key, nil)
	if err != nil {
		//key not found
		return &cachedItem{nil, cmd.ttl, time.Now(), false}
	}
	obj, err2 := cmd.fun(bytes) //unmarshall
	if err2 != nil {
		glog.Errorf("data is broken")
		return &cachedItem{nil, cmd.ttl, time.Now(), false}
	}
	return &cachedItem{obj, cmd.ttl, time.Now(), true}

}

//expiration cached objects like a redis expiration strategy
func (ldb *LevelDB) simpleExpire(depth int) {
	count := 0
	removed := 0
	//Go's map iteration order (using the range keyword) is random
	for key, val := range ldb.cache {
		if val.hasExpired() {
			//expired
			delete(ldb.cache, key)
			removed++
		}
		count++
		if count == ldb.dbConf.Options.ExpirationCount {
			break
		}
	}
	if removed >= (ldb.dbConf.Options.ExpirationCount/4) && depth < 5 {
		// retry
		glog.Warning("one more gc!")
		ldb.simpleExpire(depth + 1)
	}
}

func (ldb *LevelDB) download(switching bool) *switchDB {
	defer func() {
		//finalize
		ldb.mu.Lock()
		ldb.downloading = false
		defer ldb.mu.Unlock()
	}()
	newDB := ldb.dbConf.Storage.Download(ldb.dbConf.SaveDirPath)

	//Switching the DB
	if newDB == nil {
		glog.Warning("can't download")
		return nil
	} else if switching {
		ldb.switching <- newDB
		return nil
	} else {
		return newDB
	}
}

func (ldb *LevelDB) execGet(cmd *dbGetCmd) *dbResult {
	cached := ldb.cache[cmd.key]
	var result *cachedItem
	var hit bool
	if cached == nil || cached.hasExpired() {
		// not cached
		result = ldb.getFromLdb(cmd)
		ldb.cache[cmd.key] = result
		hit = false
	} else {
		// cached and not expired
		result = cached
		hit = true
	}
	// over capasity
	if len(ldb.cache) > ldb.dbConf.Options.Capacity {
		ldb.reset <- struct{}{}
	}
	return &dbResult{result.val, result.ok, hit}
}

func (ldb *LevelDB) run() {
	expireTick := time.NewTicker(10 * time.Second)
	defer expireTick.Stop()
	updateTick := time.NewTicker(time.Duration(ldb.dbConf.Options.UpdateInterval) * time.Second)
	defer updateTick.Stop()
	for {
		select {
		case <-ldb.reset:
			// Flush Cached data
			ldb.cache = map[string]*cachedItem{}
		case cmd := <-ldb.get:
			cmd.result <- ldb.execGet(cmd)
		case <-updateTick.C:
			// downloading leveldb from object storage
			ldb.mu.RLock()
			if !ldb.downloading {
				go ldb.download(true)
			}
			ldb.mu.RUnlock()
		case msg := <-ldb.switching:
			// swhitching
			oldDB := ldb.db
			oldPath := ldb.dbpath
			ldb.db = msg.db
			ldb.dbpath = msg.dbpath
			oldDB.Close()
			os.RemoveAll(oldPath)
		case <-ldb.expiration:
			ldb.simpleExpire(1)
		case <-expireTick.C:
			ldb.simpleExpire(1)
		case msg := <-ldb.close:
			// finalize
			ldb.db.Close()
			os.RemoveAll(ldb.dbpath)
			ldb.cache = map[string]*cachedItem{}
			msg <- struct{}{}
			break
		}
	}
}

// Get gets the value for the given key and unmarshaling by UnmarshalFunc from leveldb with cache.
func (ldb *LevelDB) Get(key string, fun UnmarshalFunc) (interface{}, bool, bool) {
	cmd := NewDBGetCmd(key, fun, ldb.dbConf.Options.CacheExpire)
	ldb.get <- cmd
	r := <-cmd.result
	if !r.ok {
		return nil, false, false
	}
	return r.val, r.ok, r.hit
}

// Close closes leveldb and deletes leveldb files.
func (ldb *LevelDB) Close() {
	ch := make(chan struct{})
	ldb.exit <- ch
	<-ch
}
