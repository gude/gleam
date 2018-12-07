package executor

import (
	"github.com/gude/gleam/flow"
	"github.com/gude/gleam/sql/expression"
)

type Executor interface {
	Exec() *flow.Dataset
	Schema() expression.Schema
}
