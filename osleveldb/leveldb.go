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
	"github.com/zenazn/goji/graceful"
	"golang.org/x/net/context"
)

type LevelDB struct {
	get         chan *dbGetCmd
	switchDB    chan *SwitchDB
	exit        chan chan struct{}
	reset       chan struct{}
	expiration  chan struct{}
	db          *leveldb.DB // LevelDB
	dbpath      string
	capasity    int64
	cache       map[string]*cachedItem
	downloading bool
	mu          sync.RWMutex
	dbConf      LevelDBConf
	name        string
}

type SwitchDB struct {
	db     *leveldb.DB
	dbpath string
}

func NewLevelDB(db *leveldb.DB, name, dbpath string, capasity int64, dbConf config.LevelDB) *LevelDB {
	return &LevelDB{
		make(chan *dbGetCmd, 999),
		make(chan *SwitchDB),
		make(chan chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
		db,
		dbpath,
		capasity,
		map[string]*cachedItem{},
		false,
		sync.RWMutex{},
		dbConf,
		name,
	}
}

func (ldb *LevelDB) getFromLdb(cmd *dbGetCmd) *cachedItem {
	key := []byte(cmd.key)
	bytes, err := ldb.db.Get(key, nil)
	if err != nil {
		//key not found
		return &cachedItem{nil, cmd.ttl, time.Now().Unix(), false}
	}
	obj, err2 := cmd.fun(bytes) //unmarshall
	if err2 != nil {
		glog.Errorf("data is broken\n")
		return &cachedItem{nil, cmd.ttl, time.Now().Unix(), false}
	}
	return &cachedItem{obj, cmd.ttl, time.Now().Unix(), true}

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
			removed += 1
		}
		count += 1
		if count == ldb.dbConf.Options.GcCount {
			break
		}
	}
	if removed >= (ldb.dbConf.Options.GcCount/4) && depth < 5 {
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

func (ldb *LevelDB) downloadFromS3(ctx context.Context, switching bool) newDB {
	defer func() {
		//finalize
		ldb.mu.Lock()
		ldb.downloading = false
		defer ldb.mu.Unlock()
	}()
	var newDB *SwitchDB = nil
	if ldb.dbConf.Type == "s3" {
		newDB = ldb.downloadFromS3(ctx)
	}
	//Switching the DB
	if newDB == nil {
		glog.Warning("can't download")
		return nil
	} else if switching {
		ldb.switchDB <- newDB
		return nil
	} else {
		return newDB
	}
}

func (ldb *LevelDB) downloadFromS3(ctx context.Context) *SwitchDB {
	//download from S3
	s3 := aws.S3(ctx, ldb.dbConf.S3.Region)
	if s3 == nil {
		glog.Warning("can't connect to s3")
		return nil
	}
	//save into temp file
	os.MkdirAll("/tmp/ldb", 0700)
	dirname := uuid.NewUUID().String()
	dirpath := path.Join("/tmp/soldb/", dirname)
	tarpath := dirpath + ".tar"
	err := s3.DownloadObject(ldb.dbConf.S3.Bucket, ldb.dbConf.S3.Path, tarpath)
	if err != nil {
		glog.Warning("download ", ldb.dbConf.S3.Path, " has failed\n")
		return nil
	}
	// unfolding
	err = unfoldTar(tarpath, dirpath)
	if err != nil {
		glog.Debug("unforlding ", ldb.dbConf.S3.Path, " has failed\n")
		return nil
	}
	os.RemoveAll(tarpath)
	db, err := leveldb.OpenFile(dirpath, nil)
	if err != nil {
		glog.Debug("opening ", ldb.dbConf.S3.Path, " has failed\n")
		return nil
	}
	glog.Debug("download ", ldb.dbConf.S3.Path, " done")
	return &SwitchDB{db, dirpath}
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
	return &dbResult{result.val, result.ok, hit}
}

func (ldb *LevelDB) run(ctx context.Context) {

	graceful.PostHook(func() {
		glog.Info("closing ", ldb.name, "...")
		ch := make(chan struct{})
		ldb.exit <- ch
		<-ch
		glog.Info("closed")
	})

	gcTick := time.NewTicker(10 * time.Second)
	defer gcTick.Stop()
	updateTick := time.NewTicker(time.Duration(ldb.dbConf.Options.UpdateInterval) * time.Second)
	defer updateTick.Stop()
	for {
		select {
		case cmd := <-ldb.get:
			cmd.result <- ldb.execGet(cmd)
		case <-ldb.reset:
			// Flush Cached data
			ldb.cache = map[string]*cachedItem{}
		case <-updateTick.C:
			// downloading leveldb from object storage
			ldb.mu.RLock()
			if !ldb.downloading {
				go ldb.download(ctx, true)
			}
			ldb.mu.RUnlock()
		case msg := <-ldb.switchDB:
			// swhitching
			oldDB := ldb.db
			oldPath := ldb.dbpath
			ldb.db = msg.db
			ldb.dbpath = msg.dbpath
			oldDB.Close()
			os.RemoveAll(oldPath)
		case <-ldb.expiration:
			ldb.simpleExpire(1)
		case <-gcTick.C:
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
