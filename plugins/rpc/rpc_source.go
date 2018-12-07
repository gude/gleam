package rpc

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/gude/gleam/filesystem"
	"github.com/gude/gleam/flow"
	"github.com/gude/gleam/pb"
	"github.com/gude/gleam/util"
)

type RPCSource struct {
	addrs []string

	prefix string
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *RPCSource) Generate(f *flow.Flow) *flow.Dataset {

	return s.genShardInfos(f).RoundRobin(s.prefix, s.PartitionCount).Map(s.prefix+".Read", registeredMapperReadShard)
}

func newRPCSource(addrs string) *RPCSource {

	s := &RPCSource{
		addrs:  "127.0.0.1:8086",
		prefix: "order",
	}

	return s
}

func (s *RPCSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(s.prefix, func(writer io.Writer, stats *pb.InstructionStat) error {
		stats.InputCounter++
		if !s.hasWildcard && !filesystem.IsDir(s.Path) {
			stats.OutputCounter++
			util.NewRow(util.Now(), encodeShardInfo(&FileShardInfo{
				FileName:  s.Path,
				FileType:  s.FileType,
				HasHeader: s.HasHeader,
				Fields:    s.Fields,
			})).WriteTo(writer)
		} else {
			virtualFiles, err := filesystem.List(s.folder)
			if err != nil {
				return fmt.Errorf("Failed to list folder %s: %v", s.folder, err)
			}
			for _, vf := range virtualFiles {
				if !s.hasWildcard || s.match(vf.Location) {
					stats.OutputCounter++
					util.NewRow(util.Now(), encodeShardInfo(&FileShardInfo{
						FileName:  vf.Location,
						FileType:  s.FileType,
						HasHeader: s.HasHeader,
						Fields:    s.Fields,
					})).WriteTo(writer)
				}
			}
		}
		return nil
	})
}

func (s *RPCSource) match(fullPath string) bool {
	baseName := filepath.Base(fullPath)
	match, _ := filepath.Match(s.fileBaseName, baseName)
	return match
}
