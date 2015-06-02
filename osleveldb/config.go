package osleveldb

type StorageS3 struct {
	Region string
	Bucket string
	Path   string
}

type StorageGridFS struct {
}

type LevelDBConf struct {
	Type     string
	Name     string
	DBPath   string
	S3       *StorageS3
	GridFS   *StorageGridFS
	Options  LevelDBOptions
	capacity int64
}
