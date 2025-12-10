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
	NoNormalize bool           // For VECTOR columns: skip auto-normalization
	DefaultExpr Expression     // For DEFAULT constraint
	CheckExpr   Expression     // For CHECK constraint
	ForeignKey  *ForeignKeyRef // For REFERENCES constraint
}

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	TableName      string
	Columns        []string       // optional column list (nil means all columns)
	Values         [][]Expression // rows of values (nil if using SelectStmt)
	SelectStmt     *SelectStmt    // SELECT subquery (nil if using Values)
	OnDuplicateKey []Assignment   // ON DUPLICATE KEY UPDATE assignments (nil if none)
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
	With    *WithClause    // optional WITH clause for CTEs
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
	IndexName   string       // Name of the index
	TableName   string       // Table to create index on
	Columns     []string     // Plain column names to index (for simple column references)
	Expressions []Expression // Expression indexes (e.g., UPPER(name), price * quantity)
	Unique      bool         // Whether this is a UNIQUE index
	Where       Expression   // Optional WHERE clause for partial indexes (nil if none)
}

func (s *CreateIndexStmt) statementNode() {}

// DropIndexStmt represents a DROP INDEX statement
type DropIndexStmt struct {
	IndexName string
	IfExists  bool
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
	Name    string    // CTE name
	Columns []string  // Optional column list
	Query   Statement // The query defining the CTE (SelectStmt or SetOperation)
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
	IfExists    bool   // IF EXISTS clause
}

func (s *DropTriggerStmt) statementNode() {}

// RaiseType represents the type of RAISE function
type RaiseType int

const (
	RaiseAbort  RaiseType = iota // RAISE(ABORT, 'message')
	RaiseIgnore                  // RAISE(IGNORE)
)

// RaiseExpr represents a RAISE expression used in triggers
type RaiseExpr struct {
	Type    RaiseType // ABORT or IGNORE
	Message string    // Error message (empty for IGNORE)
}

func (r *RaiseExpr) expressionNode() {}

// ValuesFunc represents the VALUES(column) function used in ON DUPLICATE KEY UPDATE
// It references the value that would have been inserted for the specified column
type ValuesFunc struct {
	ColumnName string
}

func (v *ValuesFunc) expressionNode() {}

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

// TruncateStmt represents a TRUNCATE TABLE statement
type TruncateStmt struct {
	TableName string // Table to truncate
}

func (s *TruncateStmt) statementNode() {}

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

// WhenClause represents a WHEN clause in a CASE expression
type WhenClause struct {
	Condition Expression // WHEN condition (searched CASE) or value (simple CASE)
	Then      Expression // THEN result
}

// CaseExpr represents a CASE expression
// Searched form: CASE WHEN condition THEN result [WHEN ...] [ELSE result] END
// Simple form: CASE operand WHEN value THEN result [WHEN ...] [ELSE result] END
type CaseExpr struct {
	Operand Expression    // nil for searched CASE, expression for simple CASE
	Whens   []*WhenClause // List of WHEN clauses
	Else    Expression    // ELSE result (nil if no ELSE)
}

func (c *CaseExpr) expressionNode() {}

// DerivedTable represents a subquery used as a table reference in FROM clause
type DerivedTable struct {
	Subquery *SelectStmt // The SELECT statement
	Alias    string      // Required alias for the derived table
}

func (d *DerivedTable) tableRefNode() {}

// TableFunction represents a table-valued function call in FROM clause
// e.g., vector_quantize_scan('table', 'column', query_vec, k)
type TableFunction struct {
	Name  string       // Function name
	Args  []Expression // Function arguments
	Alias string       // Optional alias
}

func (tf *TableFunction) tableRefNode() {}

// BeginStmt represents a BEGIN [TRANSACTION] statement
type BeginStmt struct{}

func (s *BeginStmt) statementNode() {}

// CommitStmt represents a COMMIT [TRANSACTION] statement
type CommitStmt struct{}

func (s *CommitStmt) statementNode() {}

// RollbackStmt represents a ROLLBACK [TRANSACTION] statement
type RollbackStmt struct{}

func (s *RollbackStmt) statementNode() {}

// CreateViewStmt represents a CREATE VIEW statement
type CreateViewStmt struct {
	ViewName    string      // Name of the view
	Columns     []string    // Optional column name list
	Query       *SelectStmt // The SELECT statement defining the view
	IfNotExists bool        // IF NOT EXISTS clause
}

func (s *CreateViewStmt) statementNode() {}

// DropViewStmt represents a DROP VIEW statement
type DropViewStmt struct {
	ViewName string // Name of the view to drop
	IfExists bool   // IF EXISTS clause
}

func (s *DropViewStmt) statementNode() {}

// ExplainStmt represents an EXPLAIN, EXPLAIN QUERY PLAN, or EXPLAIN ANALYZE statement
type ExplainStmt struct {
	QueryPlan bool      // true for EXPLAIN QUERY PLAN, false for EXPLAIN
	Analyze   bool      // true for EXPLAIN ANALYZE, false for regular EXPLAIN
	Statement Statement // The statement to explain
}

func (s *ExplainStmt) statementNode() {}

// SavepointStmt represents a SAVEPOINT statement
type SavepointStmt struct {
	Name string // Savepoint name
}

func (s *SavepointStmt) statementNode() {}

// RollbackToStmt represents a ROLLBACK TO [SAVEPOINT] statement
type RollbackToStmt struct {
	Name string // Savepoint name to rollback to
}

func (s *RollbackToStmt) statementNode() {}

// ReleaseStmt represents a RELEASE [SAVEPOINT] statement
type ReleaseStmt struct {
	Name string // Savepoint name to release
}

func (s *ReleaseStmt) statementNode() {}

// ElsIfClause represents an ELSIF/ELSEIF clause in an IF statement
type ElsIfClause struct {
	Condition Expression  // The ELSIF condition
	Body      []Statement // Statements to execute if condition is true
}

// IfStmt represents an IF...THEN...ELSIF...ELSE...END IF statement
type IfStmt struct {
	Condition    Expression     // The IF condition
	ThenBranch   []Statement    // Statements to execute if condition is true
	ElsIfClauses []*ElsIfClause // Optional ELSIF clauses
	ElseBranch   []Statement    // Optional ELSE branch (nil if no ELSE)
}

func (s *IfStmt) statementNode() {}

// ParamMode represents the mode of a procedure parameter
type ParamMode int

const (
	ParamModeIn    ParamMode = iota // IN parameter (default)
	ParamModeOut                    // OUT parameter
	ParamModeInOut                  // INOUT parameter
)

// ProcedureParam represents a stored procedure parameter
type ProcedureParam struct {
	Name string          // Parameter name
	Mode ParamMode       // IN, OUT, or INOUT
	Type types.ValueType // Data type
}

// CreateProcedureStmt represents a CREATE PROCEDURE statement
type CreateProcedureStmt struct {
	Name       string            // Procedure name
	Parameters []ProcedureParam  // Procedure parameters
	Body       []Statement       // Procedure body statements
}

func (s *CreateProcedureStmt) statementNode() {}

// DropProcedureStmt represents a DROP PROCEDURE statement
type DropProcedureStmt struct {
	Name     string // Procedure name
	IfExists bool   // IF EXISTS clause
}

func (s *DropProcedureStmt) statementNode() {}

// CallStmt represents a CALL statement to execute a procedure
type CallStmt struct {
	Name string       // Procedure name
	Args []Expression // Arguments to pass to the procedure
}

func (s *CallStmt) statementNode() {}

// SessionVariable represents a session variable reference (@var)
type SessionVariable struct {
	Name string // Variable name (without @)
}

func (s *SessionVariable) expressionNode() {}

// SetStmt represents a SET statement for variable assignment
type SetStmt struct {
	Variable Expression // Variable to set (SessionVariable or ColumnRef for local vars)
	Value    Expression // Value to assign
}

func (s *SetStmt) statementNode() {}

// DeclareStmt represents a DECLARE statement for local variables
type DeclareStmt struct {
	Name         string          // Variable name
	Type         types.ValueType // Variable type
	DefaultValue Expression      // Optional default value
}

func (s *DeclareStmt) statementNode() {}

// LoopStmt represents a LOOP...END LOOP statement
type LoopStmt struct {
	Label string      // Optional loop label
	Body  []Statement // Loop body statements
}

func (s *LoopStmt) statementNode() {}

// LeaveStmt represents a LEAVE statement to exit a loop
type LeaveStmt struct {
	Label string // Optional label to leave (empty for innermost loop)
}

func (s *LeaveStmt) statementNode() {}

// SelectIntoStmt represents a SELECT ... INTO statement
type SelectIntoStmt struct {
	Select    *SelectStmt  // The SELECT statement
	Variables []Expression // Variables to store results into
}

func (s *SelectIntoStmt) statementNode() {}

// DeclareCursorStmt represents a DECLARE cursor_name CURSOR FOR select_stmt
type DeclareCursorStmt struct {
	Name   string      // Cursor name
	Query  *SelectStmt // Query for the cursor
}

func (s *DeclareCursorStmt) statementNode() {}

// OpenStmt represents an OPEN cursor statement
type OpenStmt struct {
	CursorName string // Cursor to open
}

func (s *OpenStmt) statementNode() {}

// FetchStmt represents a FETCH cursor INTO variables statement
type FetchStmt struct {
	CursorName string       // Cursor to fetch from
	Variables  []Expression // Variables to fetch into
}

func (s *FetchStmt) statementNode() {}

// CloseStmt represents a CLOSE cursor statement
type CloseStmt struct {
	CursorName string // Cursor to close
}

func (s *CloseStmt) statementNode() {}

// HandlerCondition represents a condition for a handler
type HandlerCondition int

const (
	HandlerConditionNotFound     HandlerCondition = iota // NOT FOUND
	HandlerConditionSQLException                         // SQLEXCEPTION
	HandlerConditionSQLWarning                           // SQLWARNING
)

// HandlerAction represents the action type for a handler
type HandlerAction int

const (
	HandlerActionContinue HandlerAction = iota // CONTINUE HANDLER
	HandlerActionExit                          // EXIT HANDLER
)

// DeclareHandlerStmt represents DECLARE handler_action HANDLER FOR condition statement
type DeclareHandlerStmt struct {
	Action    HandlerAction    // CONTINUE or EXIT
	Condition HandlerCondition // NOT FOUND, SQLEXCEPTION, SQLWARNING
	Body      []Statement      // Handler body statements
}

func (s *DeclareHandlerStmt) statementNode() {}
