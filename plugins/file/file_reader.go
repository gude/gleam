package file

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"path/filepath"

	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/plugins/file/csv"
	"github.com/chrislusf/gleam/plugins/file/orc"
	"github.com/chrislusf/gleam/plugins/file/tsv"
	"github.com/chrislusf/gleam/plugins/file/txt"
	"github.com/chrislusf/gleam/util"
)

type FileReader interface {
	Read() (row *util.Row, err error)
	ReadHeader() (fieldNames []string, err error)
}

func Csv(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("csv", fileOrPattern, partitionCount)
}
func Txt(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("txt", fileOrPattern, partitionCount)
}
func Tsv(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("tsv", fileOrPattern, partitionCount)
}
func Orc(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("orc", fileOrPattern, partitionCount)
}

func (ds *FileShardInfo) NewReader(vf filesystem.VirtualFile) (FileReader, error) {
	// These formats require seeking, so they cannot be
	// sequentially read by a compress/* reader.
	if ds.FileType == "orc" {
		if reader, err := orc.New(vf); err == nil {
			return reader.Select(ds.Fields), nil
		} else {
			return nil, err
		}
	}

	var r io.Reader = vf
	var err error
	switch filepath.Ext(ds.FileName) {
	case ".gz":
		r, err = gzip.NewReader(r)
	case ".bz2":
		r = bzip2.NewReader(r)
	}
	if err != nil {
		return nil, err
	}

	switch ds.FileType {
	case "csv":
		return csv.New(r), nil
	case "txt":
		return txt.New(r), nil
	case "tsv":
		return tsv.New(r), nil
	}
	return nil, fmt.Errorf("File type %s is not defined.", ds.FileType)
}
