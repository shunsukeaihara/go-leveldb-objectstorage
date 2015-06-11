package osleveldb

import (
	"os"
	"path"

	"github.com/GoogleCloudPlatform/kubernetes/Godeps/_workspace/src/code.google.com/p/go-uuid/uuid"
	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
)

type StorageInterface interface {
	Download(string) *switchDB
}

type StorageS3 struct {
	Region string
	Bucket string
	Path   string
}

func (st *StorageS3) Download(saveDirPath string) *switchDB {
	//download from S3
	s3 := CreateS3Client(st.Region)
	if s3 == nil {
		glog.Warning("can't connect to s3")
		return nil
	}
	//save into temp file
	os.MkdirAll(saveDirPath, 0700)
	dirname := uuid.NewUUID().String()
	dirpath := path.Join(saveDirPath, dirname)
	tarpath := dirpath + ".tar"
	err := s3.DownloadObject(st.Bucket, st.Path, tarpath)
	if err != nil {
		glog.Warning("download ", st.Path, " has failed\n")
		return nil
	}
	// unfolding
	err = unfoldTar(tarpath, dirpath)
	if err != nil {
		glog.Info("unforlding ", st.Path, " has failed\n")
		return nil
	}
	os.RemoveAll(tarpath)
	db, err := leveldb.OpenFile(dirpath, nil)
	if err != nil {
		glog.Info("opening ", st.Path, " has failed\n")
		return nil
	}
	glog.Info("download ", st.Path, " done")
	return &switchDB{db, dirpath}
}
