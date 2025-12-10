package executor

import (
	"tur/pkg/btree"
)

// initSchemaBTree initializes or opens the schema metadata B-tree on page 1
func (e *Executor) initSchemaBTree() error {
	// Always create/open schema B-tree at page 1
	tree, err := btree.CreateAtPage(e.pager, 1)
	if err != nil {
		return err
	}
	e.schemaBTree = tree
	return nil
}
