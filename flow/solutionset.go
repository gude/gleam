package flow

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

// UpdateSolutionSet runs the mapper registered to the mapperId.
// This is used to execute pure Go code.
func (d *Dataset) UpdateSolutionSet(f func(*Dataset, io.Reader) error) *Dataset {
	step := d.Flow.AddAllToOneStep(d, nil)
	step.IsOnDriverSide = true
	step.Name = "UpdateSolutionSet"
	step.Function = func(readers []io.Reader, writers []io.Writer, stat *pb.InstructionStat) error {
		errChan := make(chan error, len(readers))
		for i, reader := range readers {
			go func(i int, reader io.Reader) {
				errChan <- f(d, reader)
			}(i, reader)
		}
		for range readers {
			err := <-errChan
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to process output: %v\n", err)
				return err
			}
		}
		return nil
	}
	return d
}

func (d *Dataset) DoUpdate() *Dataset {
	fn := func(d *Dataset, reader io.Reader) error {
		return util.TakeMessage(reader, -1, func(encodedBytes []byte) error {
			row, err := util.DecodeRow(encodedBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				return err
			}

			outputString := row.V[0].(string)
			err = d.Flow.SolutionSet.Update(outputString)
			if err != nil {
				return err
			}
			return nil
		})
	}
	return d.UpdateSolutionSet(fn)
}
