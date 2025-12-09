// pkg/sql/parser/ast.go
package parser

import (
	"tur/pkg/sql/lexer"
	"tur/pkg/types"
)

// Statement is the interface for all SQL statements
type Statement interface {
	statementNode()
}

// Expression is the interface for all expressions
type Expression interface {
	expressionNode()
}

// FKAction represents a foreign key action
type FKAction int

const (
	FKActionNoAction FKAction = iota
	FKActionRestrict
	FKActionCascade
	FKActionSetNull
	FKActionSetDefault
)

// ForeignKeyRef represents a foreign key reference
type ForeignKeyRef struct {
	RefTable  string
	RefColumn string
	OnDelete  FKAction
	OnUpdate  FKAction
}

// TableConstraintType represents the type of table-level constraint
type TableConstraintType int

const (
	TableConstraintPrimaryKey TableConstraintType = iota
	TableConstraintUnique
	TableConstraintForeignKey
	TableConstraintCheck
)

// TableConstraint represents a table-level constraint
type TableConstraint struct {
	Type       TableConstraintType
	Name       string     // Optional constraint name
	Columns    []string   // Column names for PK, UNIQUE, FK
	RefTable   string     // For FK: referenced table
	RefColumns []string   // For FK: referenced columns
	OnDelete   FKAction   // For FK: ON DELETE action
	OnUpdate   FKAction   // For FK: ON UPDATE action
	CheckExpr  Expression // For CHECK: the check expression
}

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	TableName        string
	Columns          []ColumnDef
	TableConstraints []TableConstraint
}

func (s *CreateTableStmt) statementNode() {}

// ColumnDef represents a column definition in CREATE TABLE
type ColumnDef struct {
	Name        string
	Type        types.ValueType
	PrimaryKey  bool
	NotNull     bool
	Unique      bool
	VectorDim   int
	DefaultExpr Expression     // For DEFAULT constraint
	CheckExpr   Expression     // For CHECK constraint
	ForeignKey  *ForeignKeyRef // For REFERENCES constraint
}

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	TableName  string
	Columns    []string       // optional column list (nil means all columns)
	Values     [][]Expression // rows of values (nil if using SelectStmt)
	SelectStmt *SelectStmt    // SELECT subquery (nil if using Values)
}

func (s *InsertStmt) statementNode() {}

// TableReference represents a table source in FROM clause
type TableReference interface {
	tableRefNode()
}

// Table represents a single table
type Table struct {
	Name  string
	Alias string
}

func (t *Table) tableRefNode() {}

// JoinType represents the type of join
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
)

// Join represents a join between two table references
type Join struct {
	Left      TableReference
	Right     TableReference
	Type      JoinType
	Condition Expression
}

func (j *Join) tableRefNode() {}

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	Columns []SelectColumn // * or column list
	From    TableReference
	Where   Expression    // optional WHERE clause (nil if none)
	GroupBy []Expression  // optional GROUP BY clause
	Having  Expression    // optional HAVING clause (nil if none)
	OrderBy []OrderByExpr // optional ORDER BY clause
	Limit   Expression    // optional LIMIT expression
	Offset  Expression    // optional OFFSET expression
}

func (s *SelectStmt) statementNode() {}

// SelectColumn represents a column in SELECT
type SelectColumn struct {
	Expr  Expression // Expression to select (nil if Star is true)
	Alias string     // Optional alias
	Star  bool       // true if this is *
}

// DropTableStmt represents a DROP TABLE statement
type DropTableStmt struct {
	TableName string
	IfExists  bool // IF EXISTS clause
	Cascade   bool // CASCADE clause (for foreign keys)
}

func (s *DropTableStmt) statementNode() {}

// CreateIndexStmt represents a CREATE INDEX statement
type CreateIndexStmt struct {
	IndexName string   // Name of the index
	TableName string   // Table to create index on
	Columns   []string // Column names to index
	Unique    bool     // Whether this is a UNIQUE index
}

func (s *CreateIndexStmt) statementNode() {}

// DropIndexStmt represents a DROP INDEX statement
type DropIndexStmt struct {
	IndexName string
}

func (s *DropIndexStmt) statementNode() {}

// Literal represents a literal value
type Literal struct {
	Value types.Value
}

func (l *Literal) expressionNode() {}

// ColumnRef represents a column reference
type ColumnRef struct {
	Name string
}

func (c *ColumnRef) expressionNode() {}

// BinaryExpr represents a binary expression (e.g., a = b, a AND b)
type BinaryExpr struct {
	Left  Expression
	Op    lexer.TokenType
	Right Expression
}

func (b *BinaryExpr) expressionNode() {}

// UnaryExpr represents a unary expression (e.g., -5, NOT x)
type UnaryExpr struct {
	Op    lexer.TokenType
	Right Expression
}

func (u *UnaryExpr) expressionNode() {}

// FunctionCall represents a function call expression
type FunctionCall struct {
	Name string
	Args []Expression
}

func (f *FunctionCall) expressionNode() {}

// OrderDirection represents the order direction
type OrderDirection int

const (
	OrderAsc OrderDirection = iota
	OrderDesc
)

// OrderByExpr represents an ORDER BY expression
type OrderByExpr struct {
	Expr      Expression
	Direction OrderDirection
}

// FrameMode represents the window frame mode
type FrameMode int

const (
	FrameModeRows FrameMode = iota
	FrameModeRange
)

// FrameBoundType represents the type of frame boundary
type FrameBoundType int

const (
	FrameBoundUnboundedPreceding FrameBoundType = iota
	FrameBoundPreceding
	FrameBoundCurrentRow
	FrameBoundFollowing
	FrameBoundUnboundedFollowing
)

// FrameBound represents a window frame boundary
type FrameBound struct {
	Type   FrameBoundType
	Offset Expression // For PRECEDING/FOLLOWING with offset
}

// WindowFrame represents a window frame specification
type WindowFrame struct {
	Mode       FrameMode
	StartBound *FrameBound
	EndBound   *FrameBound
}

// WindowSpec represents a window specification (OVER clause)
type WindowSpec struct {
	PartitionBy []Expression
	OrderBy     []OrderByExpr
	Frame       *WindowFrame
}

// WindowFunction represents a window function expression
type WindowFunction struct {
	Function Expression  // Usually a FunctionCall
	Over     *WindowSpec // OVER clause
}

func (w *WindowFunction) expressionNode() {}

// CTE represents a Common Table Expression
type CTE struct {
	Name    string      // CTE name
	Columns []string    // Optional column list
	Query   *SelectStmt // The SELECT query defining the CTE
}

// WithClause represents a WITH clause containing CTEs
type WithClause struct {
	Recursive bool  // Whether this is RECURSIVE
	CTEs      []CTE // List of CTEs
}

// SetOperator represents a set operation type
type SetOperator int

const (
	SetOpUnion SetOperator = iota
	SetOpIntersect
	SetOpExcept
)

// SetOperation represents a set operation (UNION, INTERSECT, EXCEPT)
type SetOperation struct {
	Left     *SelectStmt // Left query
	Operator SetOperator // Set operator
	All      bool        // Whether this is UNION ALL, etc.
	Right    *SelectStmt // Right query
}

func (s *SetOperation) statementNode() {}

// TriggerTiming represents when a trigger fires
type TriggerTiming int

const (
	TriggerBefore TriggerTiming = iota
	TriggerAfter
)

// TriggerEvent represents the event that activates a trigger
type TriggerEvent int

const (
	TriggerEventInsert TriggerEvent = iota
	TriggerEventUpdate
	TriggerEventDelete
)

// CreateTriggerStmt represents a CREATE TRIGGER statement
type CreateTriggerStmt struct {
	TriggerName string        // Name of the trigger
	Timing      TriggerTiming // BEFORE or AFTER
	Event       TriggerEvent  // INSERT, UPDATE, or DELETE
	TableName   string        // Table the trigger is on
	Actions     []Statement   // Statements to execute when triggered
}

func (s *CreateTriggerStmt) statementNode() {}

// DropTriggerStmt represents a DROP TRIGGER statement
type DropTriggerStmt struct {
	TriggerName string // Name of the trigger to drop
}

func (s *DropTriggerStmt) statementNode() {}

// PragmaStmt represents a PRAGMA statement
type PragmaStmt struct {
	Name  string     // PRAGMA name (e.g., cache_size, journal_mode)
	Value Expression // Optional value (nil for query pragmas)
}

func (s *PragmaStmt) statementNode() {}

// Assignment represents a column = expression assignment in UPDATE
type Assignment struct {
	Column string     // Column name to update
	Value  Expression // New value expression
}

// UpdateStmt represents an UPDATE statement
type UpdateStmt struct {
	TableName   string       // Table to update
	Assignments []Assignment // SET col1 = val1, col2 = val2
	Where       Expression   // Optional WHERE clause (nil if none)
}

func (s *UpdateStmt) statementNode() {}

// DeleteStmt represents a DELETE statement
type DeleteStmt struct {
	TableName string     // Table to delete from
	Where     Expression // Optional WHERE clause (nil if none)
}

func (s *DeleteStmt) statementNode() {}

// AnalyzeStmt represents an ANALYZE statement for collecting table statistics
type AnalyzeStmt struct {
	TableName string // Optional: table or index name to analyze (empty = all tables)
}

func (s *AnalyzeStmt) statementNode() {}

// AlterAction represents the type of ALTER TABLE action
type AlterAction int

const (
	AlterActionAddColumn AlterAction = iota
	AlterActionDropColumn
	AlterActionRenameTable
)

// AlterTableStmt represents an ALTER TABLE statement
type AlterTableStmt struct {
	TableName  string      // Table to alter
	Action     AlterAction // Type of alteration
	NewColumn  *ColumnDef  // For ADD COLUMN: the new column definition
	ColumnName string      // For DROP COLUMN: column to drop
	NewName    string      // For RENAME TO: new table name
}

func (s *AlterTableStmt) statementNode() {}

// SubqueryExpr represents a scalar subquery expression (SELECT ...)
type SubqueryExpr struct {
	Query *SelectStmt // The SELECT statement
}

func (s *SubqueryExpr) expressionNode() {}

// InExpr represents an IN expression (expr IN (...) or expr NOT IN (...))
type InExpr struct {
	Left     Expression   // The expression being tested (e.g., column)
	Not      bool         // True for NOT IN
	Values   []Expression // For value list: IN (1, 2, 3)
	Subquery *SelectStmt  // For subquery: IN (SELECT ...)
}

func (i *InExpr) expressionNode() {}

// ExistsExpr represents an EXISTS expression (EXISTS (...) or NOT EXISTS (...))
type ExistsExpr struct {
	Not      bool        // True for NOT EXISTS
	Subquery *SelectStmt // The SELECT statement to check
}

func (e *ExistsExpr) expressionNode() {}

// DerivedTable represents a subquery used as a table reference in FROM clause
type DerivedTable struct {
	Subquery *SelectStmt // The SELECT statement
	Alias    string      // Required alias for the derived table
}

func (d *DerivedTable) tableRefNode() {}

// BeginStmt represents a BEGIN [TRANSACTION] statement
type BeginStmt struct{}

func (s *BeginStmt) statementNode() {}

// CommitStmt represents a COMMIT [TRANSACTION] statement
type CommitStmt struct{}

func (s *CommitStmt) statementNode() {}

// RollbackStmt represents a ROLLBACK [TRANSACTION] statement
type RollbackStmt struct{}

func (s *RollbackStmt) statementNode() {}
