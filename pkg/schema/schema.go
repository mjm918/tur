// pkg/schema/schema.go
package schema

import (
	"errors"
	"sort"
	"sync"

	"tur/pkg/types"
)

var (
	ErrTableExists    = errors.New("table already exists")
	ErrTableNotFound  = errors.New("table not found")
	ErrColumnNotFound = errors.New("column not found")
)

// ColumnDef defines a table column
type ColumnDef struct {
	Name       string
	Type       types.ValueType
	PrimaryKey bool
	NotNull    bool
	Default    *types.Value // nil means no default
}

// TableDef defines a table schema
type TableDef struct {
	Name     string
	Columns  []ColumnDef
	RootPage uint32 // B-tree root page number
}

// GetColumn returns the column definition and index by name
// Returns (nil, -1) if not found
func (t *TableDef) GetColumn(name string) (*ColumnDef, int) {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i], i
		}
	}
	return nil, -1
}

// PrimaryKeyColumn returns the primary key column definition and index
// Returns (nil, -1) if no primary key
func (t *TableDef) PrimaryKeyColumn() (*ColumnDef, int) {
	for i := range t.Columns {
		if t.Columns[i].PrimaryKey {
			return &t.Columns[i], i
		}
	}
	return nil, -1
}

// ColumnCount returns the number of columns
func (t *TableDef) ColumnCount() int {
	return len(t.Columns)
}

// Catalog holds all schema definitions
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]*TableDef
}

// NewCatalog creates a new empty catalog
func NewCatalog() *Catalog {
	return &Catalog{
		tables: make(map[string]*TableDef),
	}
}

// CreateTable adds a table to the catalog
func (c *Catalog) CreateTable(table *TableDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[table.Name]; exists {
		return ErrTableExists
	}

	c.tables[table.Name] = table
	return nil
}

// DropTable removes a table from the catalog
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; !exists {
		return ErrTableNotFound
	}

	delete(c.tables, name)
	return nil
}

// GetTable returns a table definition by name
func (c *Catalog) GetTable(name string) *TableDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.tables[name]
}

// ListTables returns all table names in sorted order
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TableCount returns the number of tables
func (c *Catalog) TableCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.tables)
}
