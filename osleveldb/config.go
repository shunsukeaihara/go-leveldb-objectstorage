package osleveldb

type StorageS3 struct {
	Region string
	Bucket string
	Path   string
}

type StorageGridFS struct {
}

type LevelDBOptions struct {
	CacheExpire     int
	ExpirationCount int
	UpdateInterval  int
	Capacity        int
}

var defaultOption LevelDBOptions = LevelDBOptions{60, 100, 300, 100000}

type LevelDBConf struct {
	Type        string
	Name        string
	SaveDirPath string
	S3          *StorageS3
	GridFS      *StorageGridFS
	Options     *LevelDBOptions
}
