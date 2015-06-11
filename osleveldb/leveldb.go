package osleveldb

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/kubernetes/Godeps/_workspace/src/code.google.com/p/go-uuid/uuid"
	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
)

type LevelDB struct {
	get         chan *dbGetCmd
	switching   chan *switchDB
	exit        chan chan struct{}
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

//unfolding tar file into target directory
func unfoldTar(tarpath string, outdir string) error {
	file, err := os.Open(tarpath)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := tar.NewReader(file)

	var header *tar.Header
	os.MkdirAll(outdir, 0700)
	for {
		header, err = reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		out, err := os.Create(path.Join(outdir, path.Base(header.Name)))
		if err != nil {
			return err
		}
		defer out.Close()
		io.Copy(out, reader)
	}
	return nil
}

func (ldb *LevelDB) download(switching bool) *switchDB {
	defer func() {
		//finalize
		ldb.mu.Lock()
		ldb.downloading = false
		defer ldb.mu.Unlock()
	}()
	var newDB *switchDB
	if ldb.dbConf.Type == "s3" {
		newDB = ldb.downloadFromS3()
	} else {
		return nil
	}
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

func (ldb *LevelDB) downloadFromS3() *switchDB {
	//download from S3
	s3 := CreateS3Client(ldb.dbConf.S3.Region)
	if s3 == nil {
		glog.Warning("can't connect to s3")
		return nil
	}
	//save into temp file
	os.MkdirAll(ldb.dbConf.SaveDirPath, 0700)
	dirname := uuid.NewUUID().String()
	dirpath := path.Join(ldb.dbConf.SaveDirPath, dirname)
	tarpath := dirpath + ".tar"
	err := s3.DownloadObject(ldb.dbConf.S3.Bucket, ldb.dbConf.S3.Path, tarpath)
	if err != nil {
		glog.Warning("download ", ldb.dbConf.S3.Path, " has failed\n")
		return nil
	}
	// unfolding
	err = unfoldTar(tarpath, dirpath)
	if err != nil {
		glog.Info("unforlding ", ldb.dbConf.S3.Path, " has failed\n")
		return nil
	}
	os.RemoveAll(tarpath)
	db, err := leveldb.OpenFile(dirpath, nil)
	if err != nil {
		glog.Info("opening ", ldb.dbConf.S3.Path, " has failed\n")
		return nil
	}
	glog.Info("download ", ldb.dbConf.S3.Path, " done")
	return &switchDB{db, dirpath}
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
		case msg := <-ldb.exit:
			// finalize
			ldb.db.Close()
			os.RemoveAll(ldb.dbpath)
			ldb.cache = map[string]*cachedItem{}
			msg <- struct{}{}
			break
		}
	}
}

func (ldb *LevelDB) Get(key string, fun LdbUnmarshalFunc) (interface{}, bool, bool) {
	cmd := NewDBGetCmd(key, fun, ldb.dbConf.Options.CacheExpire)
	ldb.get <- cmd
	r := <-cmd.result
	if !r.ok {
		return nil, false, false
	}
	return r.val, r.ok, r.hit
}

func (ldb *LevelDB) Exit() {
	ch := make(chan struct{})
	ldb.exit <- ch
	<-ch
}
