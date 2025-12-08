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
	ErrIndexExists    = errors.New("index already exists")
	ErrIndexNotFound  = errors.New("index not found")
)

// Constraint violation errors
var (
	ErrNotNullViolation    = errors.New("NOT NULL constraint violation")
	ErrUniqueViolation     = errors.New("UNIQUE constraint violation")
	ErrPrimaryKeyViolation = errors.New("PRIMARY KEY constraint violation")
	ErrCheckViolation      = errors.New("CHECK constraint violation")
	ErrForeignKeyViolation = errors.New("FOREIGN KEY constraint violation")
)

// ConstraintType represents the type of constraint
type ConstraintType int

const (
	ConstraintPrimaryKey ConstraintType = iota
	ConstraintUnique
	ConstraintNotNull
	ConstraintCheck
	ConstraintForeignKey
	ConstraintDefault
)

// String returns the string representation of the constraint type
func (ct ConstraintType) String() string {
	switch ct {
	case ConstraintPrimaryKey:
		return "PRIMARY KEY"
	case ConstraintUnique:
		return "UNIQUE"
	case ConstraintNotNull:
		return "NOT NULL"
	case ConstraintCheck:
		return "CHECK"
	case ConstraintForeignKey:
		return "FOREIGN KEY"
	case ConstraintDefault:
		return "DEFAULT"
	default:
		return "UNKNOWN"
	}
}

// ForeignKeyAction represents the action to take when a referenced row is modified
type ForeignKeyAction int

const (
	FKActionNoAction ForeignKeyAction = iota
	FKActionRestrict
	FKActionCascade
	FKActionSetNull
	FKActionSetDefault
)

// String returns the string representation of the foreign key action
func (fka ForeignKeyAction) String() string {
	switch fka {
	case FKActionNoAction:
		return "NO ACTION"
	case FKActionRestrict:
		return "RESTRICT"
	case FKActionCascade:
		return "CASCADE"
	case FKActionSetNull:
		return "SET NULL"
	case FKActionSetDefault:
		return "SET DEFAULT"
	default:
		return "UNKNOWN"
	}
}

// Constraint represents a column-level constraint
type Constraint struct {
	Type            ConstraintType   // Type of constraint
	Name            string           // Optional constraint name
	CheckExpression string           // For CHECK constraints: the expression as SQL string
	DefaultValue    *types.Value     // For DEFAULT constraints: the default value
	RefTable        string           // For FOREIGN KEY: referenced table name
	RefColumn       string           // For FOREIGN KEY: referenced column name
	OnDelete        ForeignKeyAction // For FOREIGN KEY: action on delete
	OnUpdate        ForeignKeyAction // For FOREIGN KEY: action on update
}

// TableConstraint represents a table-level constraint (can span multiple columns)
type TableConstraint struct {
	Type            ConstraintType   // Type of constraint
	Name            string           // Constraint name
	Columns         []string         // Column names involved in this constraint
	CheckExpression string           // For CHECK constraints: the expression as SQL string
	RefTable        string           // For FOREIGN KEY: referenced table name
	RefColumns      []string         // For FOREIGN KEY: referenced column names
	OnDelete        ForeignKeyAction // For FOREIGN KEY: action on delete
	OnUpdate        ForeignKeyAction // For FOREIGN KEY: action on update
}

// IndexType represents the type of index
type IndexType int

const (
	IndexTypeBTree IndexType = iota
	IndexTypeHNSW
)

// String returns the string representation of the index type
func (it IndexType) String() string {
	switch it {
	case IndexTypeBTree:
		return "BTREE"
	case IndexTypeHNSW:
		return "HNSW"
	default:
		return "UNKNOWN"
	}
}

// HNSWParams holds HNSW-specific index parameters
type HNSWParams struct {
	M              int // Maximum number of connections per node (default: 16)
	EfConstruction int // Size of the dynamic candidate list during construction (default: 200)
}

// DefaultHNSWParams returns HNSW parameters with SQLite vec extension defaults
func DefaultHNSWParams() *HNSWParams {
	return &HNSWParams{
		M:              16,
		EfConstruction: 200,
	}
}

// IndexDef defines an index schema
type IndexDef struct {
	Name       string      // Index name
	TableName  string      // Table the index belongs to
	Columns    []string    // Column names in the index (order matters for multi-column)
	Type       IndexType   // Type of index (B-tree or HNSW)
	Unique     bool        // Whether the index enforces uniqueness
	RootPage   uint32      // B-tree root page number for this index
	HNSWParams *HNSWParams // HNSW-specific parameters (nil for non-HNSW indexes)
}

// ColumnDef defines a table column
type ColumnDef struct {
	Name        string
	Type        types.ValueType
	PrimaryKey  bool           // Legacy field for backward compatibility
	NotNull     bool           // Legacy field for backward compatibility
	Default     *types.Value   // nil means no default (legacy)
	VectorDim   int            // Dimension for VECTOR type, 0 for others
	Constraints []Constraint   // Column-level constraints
}

// HasConstraint returns true if the column has a constraint of the given type
func (c *ColumnDef) HasConstraint(ct ConstraintType) bool {
	for i := range c.Constraints {
		if c.Constraints[i].Type == ct {
			return true
		}
	}
	return false
}

// GetConstraint returns the first constraint of the given type, or nil if not found
func (c *ColumnDef) GetConstraint(ct ConstraintType) *Constraint {
	for i := range c.Constraints {
		if c.Constraints[i].Type == ct {
			return &c.Constraints[i]
		}
	}
	return nil
}

// TableDef defines a table schema
type TableDef struct {
	Name             string
	Columns          []ColumnDef
	RootPage         uint32            // B-tree root page number
	TableConstraints []TableConstraint // Table-level constraints
}

// GetTableConstraint returns the first table constraint of the given type, or nil if not found
func (t *TableDef) GetTableConstraint(ct ConstraintType) *TableConstraint {
	for i := range t.TableConstraints {
		if t.TableConstraints[i].Type == ct {
			return &t.TableConstraints[i]
		}
	}
	return nil
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
	mu      sync.RWMutex
	tables  map[string]*TableDef
	indexes map[string]*IndexDef
}

// NewCatalog creates a new empty catalog
func NewCatalog() *Catalog {
	return &Catalog{
		tables:  make(map[string]*TableDef),
		indexes: make(map[string]*IndexDef),
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

// CreateIndex adds an index to the catalog
func (c *Catalog) CreateIndex(index *IndexDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.indexes[index.Name]; exists {
		return ErrIndexExists
	}

	c.indexes[index.Name] = index
	return nil
}

// DropIndex removes an index from the catalog
func (c *Catalog) DropIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.indexes[name]; !exists {
		return ErrIndexNotFound
	}

	delete(c.indexes, name)
	return nil
}

// GetIndex returns an index definition by name
func (c *Catalog) GetIndex(name string) *IndexDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.indexes[name]
}

// ListIndexes returns all index names in sorted order
func (c *Catalog) ListIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.indexes))
	for name := range c.indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IndexCount returns the number of indexes
func (c *Catalog) IndexCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.indexes)
}
