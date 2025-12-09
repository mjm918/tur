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
	ErrColumnExists   = errors.New("column already exists")
	ErrIndexExists    = errors.New("index already exists")
	ErrIndexNotFound  = errors.New("index not found")
	ErrViewExists     = errors.New("view already exists")
	ErrViewNotFound   = errors.New("view not found")
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
	Name        string      // Index name
	TableName   string      // Table the index belongs to
	Columns     []string    // Column names in the index (order matters for multi-column)
	Type        IndexType   // Type of index (B-tree or HNSW)
	Unique      bool        // Whether the index enforces uniqueness
	RootPage    uint32      // B-tree root page number for this index
	HNSWParams  *HNSWParams // HNSW-specific parameters (nil for non-HNSW indexes)
	WhereClause string      // SQL predicate for partial indexes (empty for full indexes)
}

// IsPartial returns true if this is a partial index (has a WHERE clause)
func (idx *IndexDef) IsPartial() bool {
	return idx.WhereClause != ""
}

// ColumnDef defines a table column
type ColumnDef struct {
	Name        string
	Type        types.ValueType
	PrimaryKey  bool         // Legacy field for backward compatibility
	NotNull     bool         // Legacy field for backward compatibility
	Default     *types.Value // nil means no default (legacy)
	VectorDim   int          // Dimension for VECTOR type, 0 for others
	Constraints []Constraint // Column-level constraints
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

// ViewDef defines a view schema
type ViewDef struct {
	Name    string   // View name
	SQL     string   // The SQL definition (SELECT statement as text)
	Columns []string // Optional explicit column names
}

// Catalog holds all schema definitions
type Catalog struct {
	mu         sync.RWMutex
	tables     map[string]*TableDef
	indexes    map[string]*IndexDef
	views      map[string]*ViewDef
	statistics map[string]*TableStatistics
}

// NewCatalog creates a new empty catalog
func NewCatalog() *Catalog {
	return &Catalog{
		tables:     make(map[string]*TableDef),
		indexes:    make(map[string]*IndexDef),
		views:      make(map[string]*ViewDef),
		statistics: make(map[string]*TableStatistics),
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
	delete(c.statistics, name) // Also clear any statistics
	return nil
}

// AddColumn adds a column to an existing table
func (c *Catalog) AddColumn(tableName string, column ColumnDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Check if column already exists
	for _, col := range table.Columns {
		if col.Name == column.Name {
			return ErrColumnExists
		}
	}

	table.Columns = append(table.Columns, column)
	return nil
}

// DropColumn removes a column from an existing table
func (c *Catalog) DropColumn(tableName, columnName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Find and remove the column
	found := false
	newColumns := make([]ColumnDef, 0, len(table.Columns)-1)
	for _, col := range table.Columns {
		if col.Name == columnName {
			found = true
			continue
		}
		newColumns = append(newColumns, col)
	}

	if !found {
		return ErrColumnNotFound
	}

	table.Columns = newColumns
	return nil
}

// RenameTable renames a table
func (c *Catalog) RenameTable(oldName, newName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[oldName]
	if !exists {
		return ErrTableNotFound
	}

	if _, exists := c.tables[newName]; exists {
		return ErrTableExists
	}

	// Update table name
	table.Name = newName

	// Update map
	delete(c.tables, oldName)
	c.tables[newName] = table

	// Update statistics key if present
	if stats, hasStats := c.statistics[oldName]; hasStats {
		delete(c.statistics, oldName)
		c.statistics[newName] = stats
	}

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

// GetIndexesForTable returns all indexes for a given table, sorted by name
func (c *Catalog) GetIndexesForTable(tableName string) []*IndexDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var indexes []*IndexDef
	for _, idx := range c.indexes {
		if idx.TableName == tableName {
			indexes = append(indexes, idx)
		}
	}

	// Sort by name for consistent ordering
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})

	return indexes
}

// GetIndexByColumn returns the first index that includes the given column
// for the specified table. Returns nil if no matching index is found.
func (c *Catalog) GetIndexByColumn(tableName, columnName string) *IndexDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, idx := range c.indexes {
		if idx.TableName != tableName {
			continue
		}
		for _, col := range idx.Columns {
			if col == columnName {
				return idx
			}
		}
	}

	return nil
}

// CreateView adds a view to the catalog
func (c *Catalog) CreateView(view *ViewDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.views[view.Name]; exists {
		return ErrViewExists
	}

	c.views[view.Name] = view
	return nil
}

// DropView removes a view from the catalog
func (c *Catalog) DropView(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.views[name]; !exists {
		return ErrViewNotFound
	}

	delete(c.views, name)
	return nil
}

// GetView returns a view definition by name
func (c *Catalog) GetView(name string) *ViewDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.views[name]
}

// ListViews returns all view names in sorted order
func (c *Catalog) ListViews() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.views))
	for name := range c.views {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ViewCount returns the number of views
func (c *Catalog) ViewCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.views)
}

// GetTableStatistics returns the statistics for a table, or nil if not found
func (c *Catalog) GetTableStatistics(tableName string) *TableStatistics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.statistics[tableName]
}

// UpdateTableStatistics updates the statistics for a table
// Returns ErrTableNotFound if the table doesn't exist
func (c *Catalog) UpdateTableStatistics(tableName string, stats *TableStatistics) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[tableName]; !exists {
		return ErrTableNotFound
	}

	c.statistics[tableName] = stats
	return nil
}

// GetColumnStatistics returns the statistics for a specific column, or nil if not found
func (c *Catalog) GetColumnStatistics(tableName, columnName string) *ColumnStatistics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tableStats := c.statistics[tableName]
	if tableStats == nil || tableStats.ColumnStats == nil {
		return nil
	}

	return tableStats.ColumnStats[columnName]
}

// ForeignKeyReference represents a foreign key that references a specific table/column
type ForeignKeyReference struct {
	ReferencingTable   string           // Table containing the FK
	ReferencingColumn  string           // Column with the FK (for column-level)
	ReferencingColumns []string         // Columns with FK (for table-level composite)
	OnDelete           ForeignKeyAction // Action on delete
	OnUpdate           ForeignKeyAction // Action on update
}

// GetForeignKeyReferences returns all foreign key references to a specific table and column
// This is used to check FK constraints before DELETE operations
func (c *Catalog) GetForeignKeyReferences(tableName, columnName string) []ForeignKeyReference {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var refs []ForeignKeyReference

	for _, table := range c.tables {
		// Check column-level constraints
		for _, col := range table.Columns {
			for _, constraint := range col.Constraints {
				if constraint.Type == ConstraintForeignKey &&
					constraint.RefTable == tableName &&
					constraint.RefColumn == columnName {
					refs = append(refs, ForeignKeyReference{
						ReferencingTable:  table.Name,
						ReferencingColumn: col.Name,
						OnDelete:          constraint.OnDelete,
						OnUpdate:          constraint.OnUpdate,
					})
				}
			}
		}

		// Check table-level constraints
		for _, tc := range table.TableConstraints {
			if tc.Type == ConstraintForeignKey && tc.RefTable == tableName {
				// Check if the referenced column is in the list
				for _, refCol := range tc.RefColumns {
					if refCol == columnName {
						refs = append(refs, ForeignKeyReference{
							ReferencingTable:   table.Name,
							ReferencingColumns: tc.Columns,
							OnDelete:           tc.OnDelete,
							OnUpdate:           tc.OnUpdate,
						})
						break
					}
				}
			}
		}
	}

	return refs
}
