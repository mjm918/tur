package executor

import (
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/dbfile"
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

// persistSchemaEntry writes a schema entry to the schema B-tree
func (e *Executor) persistSchemaEntry(entry *dbfile.SchemaEntry) error {
	if e.schemaBTree == nil {
		return fmt.Errorf("schema B-tree not initialized")
	}

	// Encode entry to binary
	data := entry.Encode()

	// Use entry name as key
	key := []byte(entry.Name)

	// Insert into schema B-tree
	return e.schemaBTree.Insert(key, data)
}

// getSchemaEntry retrieves a schema entry by name
func (e *Executor) getSchemaEntry(name string) (*dbfile.SchemaEntry, error) {
	if e.schemaBTree == nil {
		return nil, fmt.Errorf("schema B-tree not initialized")
	}

	key := []byte(name)
	value, err := e.schemaBTree.Get(key)
	if err != nil {
		if err == btree.ErrKeyNotFound {
			return nil, fmt.Errorf("schema entry %s not found", name)
		}
		return nil, err
	}

	return dbfile.DecodeSchemaEntry(value)
}

// deleteSchemaEntry removes a schema entry from the schema B-tree
func (e *Executor) deleteSchemaEntry(name string) error {
	if e.schemaBTree == nil {
		return fmt.Errorf("schema B-tree not initialized")
	}

	key := []byte(name)
	return e.schemaBTree.Delete(key)
}
