package osleveldb

type StorageS3 struct {
	Region string
	Bucket string
	Path   string
}

type StorageGridFS struct {
}

type LevelDBOptions struct {
	CacheExpire     int64
	ExpirationCount int64
	UpdateInterval  int64
	Capacity        int64
}

var defaultOption LevelDBOptions = LevelDBOptions{60, 100, 300, 100000}

type LevelDBConf struct {
	Type        string
	Name        string
	SaveDirPath string
	S3          *StorageS3
	GridFS      *StorageGridFS
	Options     LevelDBOptions
}
