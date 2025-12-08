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
	TableName string
	Columns   []string       // optional column list (nil means all columns)
	Values    [][]Expression // rows of values
}

func (s *InsertStmt) statementNode() {}

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	Columns []SelectColumn // * or column list
	From    string
	Where   Expression // optional WHERE clause (nil if none)
}

func (s *SelectStmt) statementNode() {}

// SelectColumn represents a column in SELECT
type SelectColumn struct {
	Star bool   // true if this is *
	Name string // column name (empty if Star)
}

// DropTableStmt represents a DROP TABLE statement
type DropTableStmt struct {
	TableName string
}

func (s *DropTableStmt) statementNode() {}

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
