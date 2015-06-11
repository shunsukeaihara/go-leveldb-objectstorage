package osleveldb

type LevelDBOptions struct {
	CacheExpire     int
	ExpirationCount int
	UpdateInterval  int
	Capacity        int
}

var defaultOption LevelDBOptions = LevelDBOptions{60, 100, 300, 100000}

type LevelDBConf struct {
	Name        string
	SaveDirPath string
	Storage     StorageInterface
	Options     *LevelDBOptions
}
