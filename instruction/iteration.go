package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetIteration() != nil {
			return NewIteration()
		}
		return nil
	})
}

type Iteration struct {
}

func NewIteration() *Iteration {
	return &Iteration{}
}

func (b *Iteration) Name(prefix string) string {
	return prefix + ".Iteration"
}

func (b *Iteration) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoIteration(readers[0], writers, stats)
	}
}

func (b *Iteration) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Iteration: &pb.Instruction_Iteration{},
	}
}

func (b *Iteration) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoIteration(reader io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return util.ProcessMessage(reader, func(data []byte) error {
		stats.InputCounter++
		for _, writer := range writers {
			stats.OutputCounter++
			util.WriteMessage(writer, data)
		}
		return nil
	})
}
