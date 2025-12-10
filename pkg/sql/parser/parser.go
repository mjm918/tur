// pkg/sql/parser/parser.go
package parser

import (
	"fmt"
	"strconv"
	"strings"

	"tur/pkg/sql/lexer"
	"tur/pkg/types"
)

// Parser is a recursive descent SQL parser
type Parser struct {
	lexer *lexer.Lexer
	cur   lexer.Token
	peek  lexer.Token
}

// New creates a new Parser for the given SQL input
func New(input string) *Parser {
	p := &Parser{lexer: lexer.New(input)}
	// Read two tokens to initialize cur and peek
	p.nextToken()
	p.nextToken()
	return p
}

// nextToken advances to the next token
func (p *Parser) nextToken() {
	p.cur = p.peek
	p.peek = p.lexer.NextToken()
}

// Parse parses the input and returns a Statement
func (p *Parser) Parse() (Statement, error) {
	switch p.cur.Type {
	case lexer.CREATE:
		return p.parseCreate()
	case lexer.INSERT:
		return p.parseInsert()
	case lexer.SELECT:
		return p.parseSelect()
	case lexer.WITH:
		return p.parseWith()
	case lexer.DROP:
		return p.parseDrop()
	case lexer.UPDATE:
		return p.parseUpdate()
	case lexer.DELETE:
		return p.parseDelete()
	case lexer.TRUNCATE:
		return p.parseTruncate()
	case lexer.ANALYZE:
		return p.parseAnalyze()
	case lexer.ALTER:
		return p.parseAlter()
	case lexer.BEGIN:
		return p.parseBegin()
	case lexer.COMMIT:
		return p.parseCommit()
	case lexer.ROLLBACK:
		return p.parseRollback()
	case lexer.EXPLAIN:
		return p.parseExplain()
	case lexer.SAVEPOINT:
		return p.parseSavepoint()
	case lexer.RELEASE:
		return p.parseRelease()
	case lexer.IF:
		return p.parseIfStmt()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.cur.Literal)
	}
}

// ParseExpression parses a single expression from the input.
// This is useful for evaluating stored expression strings (e.g., in expression indexes).
func (p *Parser) ParseExpression() (Expression, error) {
	return p.parseExpression(LOWEST)
}

// parseCreate handles CREATE TABLE, CREATE INDEX, CREATE VIEW, and CREATE TRIGGER statements
func (p *Parser) parseCreate() (Statement, error) {
	p.nextToken() // consume CREATE

	switch p.cur.Type {
	case lexer.TABLE:
		return p.parseCreateTableBody()
	case lexer.INDEX:
		return p.parseCreateIndex(false)
	case lexer.UNIQUE:
		// CREATE UNIQUE INDEX
		if !p.expectPeek(lexer.INDEX) {
			return nil, fmt.Errorf("expected INDEX after UNIQUE, got %s", p.peek.Literal)
		}
		return p.parseCreateIndex(true)
	case lexer.VIEW:
		return p.parseCreateView(false)
	case lexer.TRIGGER:
		return p.parseCreateTrigger()
	case lexer.IF:
		// CREATE IF NOT EXISTS VIEW (SQLite extension)
		if !p.expectPeek(lexer.NOT) {
			return nil, fmt.Errorf("expected NOT after IF, got %s", p.peek.Literal)
		}
		if !p.expectPeek(lexer.EXISTS) {
			return nil, fmt.Errorf("expected EXISTS after NOT, got %s", p.peek.Literal)
		}
		if p.peekIs(lexer.VIEW) {
			p.nextToken() // consume VIEW
			return p.parseCreateView(true)
		}
		return nil, fmt.Errorf("expected VIEW after IF NOT EXISTS, got %s", p.peek.Literal)
	default:
		return nil, fmt.Errorf("expected TABLE, INDEX, VIEW, TRIGGER, or UNIQUE after CREATE, got %s", p.cur.Literal)
	}
}

// parseDrop handles DROP TABLE, DROP INDEX, DROP VIEW, and DROP TRIGGER statements
func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // consume DROP

	switch p.cur.Type {
	case lexer.TABLE:
		return p.parseDropTableBody()
	case lexer.INDEX:
		return p.parseDropIndex()
	case lexer.VIEW:
		return p.parseDropView()
	case lexer.TRIGGER:
		return p.parseDropTrigger()
	default:
		return nil, fmt.Errorf("expected TABLE, INDEX, VIEW, or TRIGGER after DROP, got %s", p.cur.Literal)
	}
}

// parseCreateTableBody parses: TABLE name (column_def, ..., [table_constraints])
// Called after CREATE has been consumed and current token is TABLE
func (p *Parser) parseCreateTableBody() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}

	// Current token is TABLE, move to table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// (
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(', got %s", p.peek.Literal)
	}

	// column definitions and table constraints
	for {
		p.nextToken()

		// Check if this is a table-level constraint
		if p.isTableConstraintStart() {
			constraint, err := p.parseTableConstraint()
			if err != nil {
				return nil, err
			}
			stmt.TableConstraints = append(stmt.TableConstraints, constraint)
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col)
		}

		if p.peekIs(lexer.COMMA) {
			p.nextToken() // consume ,
		} else {
			break
		}
	}

	if len(stmt.Columns) == 0 {
		return nil, fmt.Errorf("expected at least one column definition")
	}

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' or ',', got %s", p.peek.Literal)
	}

	return stmt, nil
}

// isTableConstraintStart checks if the current token starts a table constraint
func (p *Parser) isTableConstraintStart() bool {
	switch p.cur.Type {
	case lexer.PRIMARY, lexer.UNIQUE, lexer.FOREIGN, lexer.CHECK, lexer.CONSTRAINT:
		return true
	default:
		return false
	}
}

// parseTableConstraint parses a table-level constraint
func (p *Parser) parseTableConstraint() (TableConstraint, error) {
	constraint := TableConstraint{}

	// Check for optional CONSTRAINT name
	if p.cur.Type == lexer.CONSTRAINT {
		if !p.expectPeek(lexer.IDENT) {
			return constraint, fmt.Errorf("expected constraint name after CONSTRAINT")
		}
		constraint.Name = p.cur.Literal
		p.nextToken() // move to the constraint type
	}

	switch p.cur.Type {
	case lexer.PRIMARY:
		return p.parseTablePrimaryKey(constraint)
	case lexer.UNIQUE:
		return p.parseTableUnique(constraint)
	case lexer.FOREIGN:
		return p.parseTableForeignKey(constraint)
	case lexer.CHECK:
		return p.parseTableCheck(constraint)
	default:
		return constraint, fmt.Errorf("unexpected table constraint type: %s", p.cur.Literal)
	}
}

// parseTablePrimaryKey parses: PRIMARY KEY (col1, col2, ...)
func (p *Parser) parseTablePrimaryKey(constraint TableConstraint) (TableConstraint, error) {
	constraint.Type = TableConstraintPrimaryKey

	// PRIMARY
	if !p.expectPeek(lexer.KEY) {
		return constraint, fmt.Errorf("expected KEY after PRIMARY")
	}

	// (
	if !p.expectPeek(lexer.LPAREN) {
		return constraint, fmt.Errorf("expected '(' after PRIMARY KEY")
	}

	cols, err := p.parseIdentList()
	if err != nil {
		return constraint, err
	}
	constraint.Columns = cols

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return constraint, fmt.Errorf("expected ')' after column list")
	}

	return constraint, nil
}

// parseTableUnique parses: UNIQUE (col1, col2, ...)
func (p *Parser) parseTableUnique(constraint TableConstraint) (TableConstraint, error) {
	constraint.Type = TableConstraintUnique

	// (
	if !p.expectPeek(lexer.LPAREN) {
		return constraint, fmt.Errorf("expected '(' after UNIQUE")
	}

	cols, err := p.parseIdentList()
	if err != nil {
		return constraint, err
	}
	constraint.Columns = cols

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return constraint, fmt.Errorf("expected ')' after column list")
	}

	return constraint, nil
}

// parseTableForeignKey parses: FOREIGN KEY (cols) REFERENCES table(cols) [ON DELETE action] [ON UPDATE action]
func (p *Parser) parseTableForeignKey(constraint TableConstraint) (TableConstraint, error) {
	constraint.Type = TableConstraintForeignKey

	// KEY
	if !p.expectPeek(lexer.KEY) {
		return constraint, fmt.Errorf("expected KEY after FOREIGN")
	}

	// (
	if !p.expectPeek(lexer.LPAREN) {
		return constraint, fmt.Errorf("expected '(' after FOREIGN KEY")
	}

	cols, err := p.parseIdentList()
	if err != nil {
		return constraint, err
	}
	constraint.Columns = cols

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return constraint, fmt.Errorf("expected ')' after column list")
	}

	// REFERENCES
	if !p.expectPeek(lexer.REFERENCES) {
		return constraint, fmt.Errorf("expected REFERENCES after FOREIGN KEY columns")
	}

	// table name
	if !p.expectPeek(lexer.IDENT) {
		return constraint, fmt.Errorf("expected table name after REFERENCES")
	}
	constraint.RefTable = p.cur.Literal

	// (ref columns)
	if !p.expectPeek(lexer.LPAREN) {
		return constraint, fmt.Errorf("expected '(' after referenced table name")
	}

	refCols, err := p.parseIdentList()
	if err != nil {
		return constraint, err
	}
	constraint.RefColumns = refCols

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return constraint, fmt.Errorf("expected ')' after referenced columns")
	}

	// Optional ON DELETE/UPDATE actions
	constraint.OnDelete, constraint.OnUpdate, err = p.parseFKActions()
	if err != nil {
		return constraint, err
	}

	return constraint, nil
}

// parseTableCheck parses: CHECK (expression)
func (p *Parser) parseTableCheck(constraint TableConstraint) (TableConstraint, error) {
	constraint.Type = TableConstraintCheck

	// (
	if !p.expectPeek(lexer.LPAREN) {
		return constraint, fmt.Errorf("expected '(' after CHECK")
	}

	p.nextToken() // move to expression start
	expr, err := p.parseExpression(LOWEST)
	if err != nil {
		return constraint, err
	}
	constraint.CheckExpr = expr

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return constraint, fmt.Errorf("expected ')' after CHECK expression")
	}

	return constraint, nil
}

// parseColumnDef parses: name TYPE [constraints...]
func (p *Parser) parseColumnDef() (ColumnDef, error) {
	col := ColumnDef{}

	// Column name
	if p.cur.Type != lexer.IDENT {
		return col, fmt.Errorf("expected column name, got %s", p.cur.Literal)
	}
	col.Name = p.cur.Literal

	// Type
	p.nextToken()
	colType, dim, err := p.parseColumnType()
	if err != nil {
		return col, err
	}
	col.Type = colType
	col.VectorDim = dim

	// Optional constraints
	for {
		if p.peekIs(lexer.PRIMARY) {
			p.nextToken() // PRIMARY
			if !p.expectPeek(lexer.KEY) {
				return col, fmt.Errorf("expected KEY after PRIMARY, got %s", p.peek.Literal)
			}
			col.PrimaryKey = true
		} else if p.peekIs(lexer.NOT) {
			p.nextToken() // NOT
			if !p.expectPeek(lexer.NULL_KW) {
				return col, fmt.Errorf("expected NULL after NOT, got %s", p.peek.Literal)
			}
			col.NotNull = true
		} else if p.peekIs(lexer.UNIQUE) {
			p.nextToken() // UNIQUE
			col.Unique = true
		} else if p.peekIs(lexer.DEFAULT) {
			p.nextToken() // DEFAULT
			p.nextToken() // move to value
			expr, err := p.parsePrefixExpression()
			if err != nil {
				return col, fmt.Errorf("expected expression after DEFAULT: %v", err)
			}
			col.DefaultExpr = expr
		} else if p.peekIs(lexer.CHECK) {
			p.nextToken() // CHECK
			if !p.expectPeek(lexer.LPAREN) {
				return col, fmt.Errorf("expected '(' after CHECK")
			}
			p.nextToken() // move to expression start
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return col, err
			}
			col.CheckExpr = expr
			if !p.expectPeek(lexer.RPAREN) {
				return col, fmt.Errorf("expected ')' after CHECK expression")
			}
		} else if p.peekIs(lexer.REFERENCES) {
			p.nextToken() // REFERENCES
			fk, err := p.parseColumnForeignKey()
			if err != nil {
				return col, err
			}
			col.ForeignKey = fk
		} else if p.peekIs(lexer.NONORMALIZE) {
			p.nextToken() // NONORMALIZE
			col.NoNormalize = true
		} else {
			break
		}
	}

	return col, nil
}

// parseColumnForeignKey parses: REFERENCES table(column) [ON DELETE action] [ON UPDATE action]
func (p *Parser) parseColumnForeignKey() (*ForeignKeyRef, error) {
	fk := &ForeignKeyRef{}

	// table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name after REFERENCES")
	}
	fk.RefTable = p.cur.Literal

	// (column)
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(' after table name")
	}

	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected column name in REFERENCES")
	}
	fk.RefColumn = p.cur.Literal

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' after column name")
	}

	// Optional ON DELETE/UPDATE actions
	var err error
	fk.OnDelete, fk.OnUpdate, err = p.parseFKActions()
	if err != nil {
		return nil, err
	}

	return fk, nil
}

// parseFKActions parses: [ON DELETE action] [ON UPDATE action]
func (p *Parser) parseFKActions() (FKAction, FKAction, error) {
	onDelete := FKActionNoAction
	onUpdate := FKActionNoAction

	for p.peekIs(lexer.ON) {
		p.nextToken() // ON

		if p.peekIs(lexer.DELETE) {
			p.nextToken() // DELETE
			action, err := p.parseFKAction()
			if err != nil {
				return onDelete, onUpdate, err
			}
			onDelete = action
		} else if p.peekIs(lexer.UPDATE) {
			p.nextToken() // UPDATE
			action, err := p.parseFKAction()
			if err != nil {
				return onDelete, onUpdate, err
			}
			onUpdate = action
		} else {
			return onDelete, onUpdate, fmt.Errorf("expected DELETE or UPDATE after ON")
		}
	}

	return onDelete, onUpdate, nil
}

// parseFKAction parses: CASCADE | RESTRICT | SET NULL | SET DEFAULT | NO ACTION
func (p *Parser) parseFKAction() (FKAction, error) {
	p.nextToken()

	switch p.cur.Type {
	case lexer.CASCADE:
		return FKActionCascade, nil
	case lexer.RESTRICT:
		return FKActionRestrict, nil
	case lexer.SET:
		if p.peekIs(lexer.NULL_KW) {
			p.nextToken()
			return FKActionSetNull, nil
		}
		if p.peekIs(lexer.DEFAULT) {
			p.nextToken()
			return FKActionSetDefault, nil
		}
		return FKActionNoAction, fmt.Errorf("expected NULL or DEFAULT after SET")
	case lexer.IDENT:
		if p.cur.Literal == "NO" && p.peekIs(lexer.ACTION) {
			p.nextToken()
			return FKActionNoAction, nil
		}
		return FKActionNoAction, fmt.Errorf("unexpected foreign key action: %s", p.cur.Literal)
	default:
		return FKActionNoAction, fmt.Errorf("unexpected foreign key action: %s", p.cur.Literal)
	}
}

// parseColumnType parses a column type and optional dimension
func (p *Parser) parseColumnType() (types.ValueType, int, error) {
	switch p.cur.Type {
	case lexer.INT_TYPE, lexer.INTEGER:
		return types.TypeInt, 0, nil
	case lexer.TEXT_TYPE:
		return types.TypeText, 0, nil
	case lexer.FLOAT_TYPE, lexer.REAL:
		return types.TypeFloat, 0, nil
	case lexer.BLOB_TYPE:
		return types.TypeBlob, 0, nil
	case lexer.VECTOR:
		// Expect (dimension)
		if !p.expectPeek(lexer.LPAREN) {
			return types.TypeVector, 0, fmt.Errorf("expected '(' after VECTOR")
		}

		if !p.expectPeek(lexer.INT) {
			return types.TypeVector, 0, fmt.Errorf("expected dimension integer, got %s", p.peek.Literal)
		}

		dim, err := strconv.Atoi(p.cur.Literal)
		if err != nil {
			return types.TypeVector, 0, fmt.Errorf("invalid dimension: %s", p.cur.Literal)
		}

		if dim <= 0 {
			return types.TypeVector, 0, fmt.Errorf("dimension must be positive, got %d", dim)
		}

		if !p.expectPeek(lexer.RPAREN) {
			return types.TypeVector, 0, fmt.Errorf("expected ')' after dimension")
		}

		return types.TypeVector, dim, nil
	default:
		return types.TypeNull, 0, fmt.Errorf("expected type, got %s", p.cur.Literal)
	}
}

// parseInsert parses: INSERT INTO table [(columns)] VALUES (values), ... | SELECT ...
func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{}

	// INSERT
	if !p.expectPeek(lexer.INTO) {
		return nil, fmt.Errorf("expected INTO, got %s", p.peek.Literal)
	}

	// INTO table
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// Optional column list
	if p.peekIs(lexer.LPAREN) {
		p.nextToken() // (
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols

		if !p.expectPeek(lexer.RPAREN) {
			return nil, fmt.Errorf("expected ')', got %s", p.peek.Literal)
		}
	}

	// VALUES or SELECT
	p.nextToken()

	if p.cur.Type == lexer.VALUES {
		// Parse VALUES (value_list), ...
		for {
			if !p.expectPeek(lexer.LPAREN) {
				return nil, fmt.Errorf("expected '(', got %s", p.peek.Literal)
			}

			row, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			stmt.Values = append(stmt.Values, row)

			if !p.expectPeek(lexer.RPAREN) {
				return nil, fmt.Errorf("expected ')', got %s", p.peek.Literal)
			}

			if p.peekIs(lexer.COMMA) {
				p.nextToken() // consume ,
			} else {
				break
			}
		}
	} else if p.cur.Type == lexer.SELECT {
		// Parse SELECT statement
		// Current token is SELECT, advance to start of column list
		p.nextToken()
		selectStmt, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}
		stmt.SelectStmt = selectStmt
	} else {
		return nil, fmt.Errorf("expected VALUES or SELECT, got %s", p.cur.Literal)
	}

	// Check for ON DUPLICATE KEY UPDATE clause
	if p.peekIs(lexer.ON) {
		p.nextToken() // consume ON

		// Expect DUPLICATE
		if !p.expectPeek(lexer.DUPLICATE) {
			return nil, fmt.Errorf("expected DUPLICATE after ON, got %s", p.peek.Literal)
		}

		// Expect KEY
		if !p.expectPeek(lexer.KEY) {
			return nil, fmt.Errorf("expected KEY after DUPLICATE, got %s", p.peek.Literal)
		}

		// Expect UPDATE
		if !p.expectPeek(lexer.UPDATE) {
			return nil, fmt.Errorf("expected UPDATE after KEY, got %s", p.peek.Literal)
		}

		// Parse assignments: col1 = val1, col2 = val2, ...
		var assignments []Assignment
		for {
			// Column name
			if !p.expectPeek(lexer.IDENT) {
				return nil, fmt.Errorf("expected column name in ON DUPLICATE KEY UPDATE, got %s", p.peek.Literal)
			}
			colName := p.cur.Literal

			// =
			if !p.expectPeek(lexer.EQ) {
				return nil, fmt.Errorf("expected '=' after column name, got %s", p.peek.Literal)
			}

			// Expression
			p.nextToken()
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return nil, fmt.Errorf("failed to parse expression in ON DUPLICATE KEY UPDATE: %w", err)
			}

			assignments = append(assignments, Assignment{
				Column: colName,
				Value:  expr,
			})

			// Check for more assignments
			if !p.peekIs(lexer.COMMA) {
				break
			}
			p.nextToken() // consume comma
		}

		stmt.OnDuplicateKey = assignments
	}

	return stmt, nil
}

// parseUpdate parses: UPDATE table SET col1=val1, col2=val2 [WHERE expr]
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{}

	// UPDATE - consume and move to table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name after UPDATE, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// SET
	if !p.expectPeek(lexer.SET) {
		return nil, fmt.Errorf("expected SET, got %s", p.peek.Literal)
	}

	// Parse assignments: col1 = val1, col2 = val2, ...
	for {
		// Column name
		if !p.expectPeek(lexer.IDENT) {
			return nil, fmt.Errorf("expected column name, got %s", p.peek.Literal)
		}
		column := p.cur.Literal

		// =
		if !p.expectPeek(lexer.EQ) {
			return nil, fmt.Errorf("expected '=' after column name, got %s", p.peek.Literal)
		}

		// Value expression
		p.nextToken()
		value, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}

		stmt.Assignments = append(stmt.Assignments, Assignment{
			Column: column,
			Value:  value,
		})

		// Check for more assignments or end
		if !p.peekIs(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	// Optional WHERE clause
	if p.peekIs(lexer.WHERE) {
		p.nextToken() // consume WHERE
		p.nextToken() // move to expression
		where, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// parseDelete parses: DELETE FROM table [WHERE expr]
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}

	// DELETE - consume and expect FROM
	if !p.expectPeek(lexer.FROM) {
		return nil, fmt.Errorf("expected FROM after DELETE, got %s", p.peek.Literal)
	}

	// Table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name after FROM, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// Optional WHERE clause
	if p.peekIs(lexer.WHERE) {
		p.nextToken() // consume WHERE
		p.nextToken() // move to expression
		where, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// parseTruncate parses: TRUNCATE TABLE table_name
func (p *Parser) parseTruncate() (*TruncateStmt, error) {
	stmt := &TruncateStmt{}

	// TRUNCATE - consume and expect TABLE
	if !p.expectPeek(lexer.TABLE) {
		return nil, fmt.Errorf("expected TABLE after TRUNCATE, got %s", p.peek.Literal)
	}

	// Table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name after TABLE, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	return stmt, nil
}

// parseSelect parses: SELECT columns FROM table [WHERE expr] [UNION|INTERSECT|EXCEPT ...]
func (p *Parser) parseSelect() (Statement, error) {
	// SELECT
	p.nextToken()

	left, err := p.parseSelectBody()
	if err != nil {
		return nil, err
	}

	// Check for set operations (UNION, INTERSECT, EXCEPT)
	return p.parseSetOperations(left)
}

// parseSetOperations checks for and parses set operations after a SELECT
func (p *Parser) parseSetOperations(left *SelectStmt) (Statement, error) {
	// Check for set operation keywords
	if !p.peekIs(lexer.UNION) && !p.peekIs(lexer.INTERSECT) && !p.peekIs(lexer.EXCEPT) {
		return left, nil
	}

	p.nextToken() // consume set operator

	var op SetOperator
	switch p.cur.Type {
	case lexer.UNION:
		op = SetOpUnion
	case lexer.INTERSECT:
		op = SetOpIntersect
	case lexer.EXCEPT:
		op = SetOpExcept
	}

	// Check for ALL modifier
	all := false
	if p.peekIs(lexer.ALL) {
		p.nextToken() // consume ALL
		all = true
	}

	// Parse the right SELECT statement
	if !p.expectPeek(lexer.SELECT) {
		return nil, fmt.Errorf("expected SELECT after %s, got %s", p.cur.Literal, p.peek.Literal)
	}
	p.nextToken() // consume SELECT, move to columns

	right, err := p.parseSelectBody()
	if err != nil {
		return nil, err
	}

	setOp := &SetOperation{
		Left:     left,
		Operator: op,
		All:      all,
		Right:    right,
	}

	return setOp, nil
}

// parseSelectBody parses the body of a SELECT statement (after SELECT keyword)
func (p *Parser) parseSelectBody() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	// Columns
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM clause is optional (allows SELECT 1+1, SELECT function() without FROM)
	if p.peekIs(lexer.FROM) {
		p.nextToken() // consume FROM
		tableRef, err := p.parseTableReference()
		if err != nil {
			return nil, err
		}
		stmt.From = tableRef
	}

	// Optional WHERE
	if p.peekIs(lexer.WHERE) {
		p.nextToken() // WHERE
		p.nextToken() // move to expression start

		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// Optional GROUP BY
	if p.peekIs(lexer.GROUP) {
		p.nextToken() // GROUP
		if !p.expectPeek(lexer.BY) {
			return nil, fmt.Errorf("expected BY after GROUP, got %s", p.peek.Literal)
		}

		groupBy, err := p.parseGroupByList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
	}

	// Optional HAVING
	if p.peekIs(lexer.HAVING) {
		p.nextToken() // HAVING
		p.nextToken() // move to expression start

		having, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Having = having
	}

	// Optional ORDER BY
	if p.peekIs(lexer.ORDER) {
		p.nextToken() // ORDER
		if !p.expectPeek(lexer.BY) {
			return nil, fmt.Errorf("expected BY after ORDER, got %s", p.peek.Literal)
		}

		orderBy, err := p.parseOrderByList()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// Optional LIMIT
	if p.peekIs(lexer.LIMIT) {
		p.nextToken() // LIMIT
		p.nextToken() // move to expression start

		limit, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Limit = limit
	}

	// Optional OFFSET
	if p.peekIs(lexer.OFFSET) {
		p.nextToken() // OFFSET
		p.nextToken() // move to expression start

		offset, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Offset = offset
	}

	return stmt, nil
}

// parseGroupByList parses: expr [, expr ...]
func (p *Parser) parseGroupByList() ([]Expression, error) {
	var groupBy []Expression

	for {
		p.nextToken() // move to expression start

		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}

		groupBy = append(groupBy, expr)

		if !p.peekIs(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	return groupBy, nil
}

// parseOrderByList parses: expr [ASC|DESC] [, expr [ASC|DESC] ...]
func (p *Parser) parseOrderByList() ([]OrderByExpr, error) {
	var orderBy []OrderByExpr

	for {
		p.nextToken() // move to expression start

		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}

		direction := OrderAsc // Default is ASC
		if p.peekIs(lexer.ASC) {
			p.nextToken()
		} else if p.peekIs(lexer.DESC) {
			p.nextToken()
			direction = OrderDesc
		}

		orderBy = append(orderBy, OrderByExpr{
			Expr:      expr,
			Direction: direction,
		})

		if !p.peekIs(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	return orderBy, nil
}

// parseTableReference parses: table_factor [JOIN table_factor ON ...]
func (p *Parser) parseTableReference() (TableReference, error) {
	left, err := p.parseTableFactor()
	if err != nil {
		return nil, err
	}

	// Loop to handle multiple joins: t1 JOIN t2 JOIN t3 ... -> ((t1 JOIN t2) JOIN t3)
	for {
		if !p.isJoinStart() {
			break
		}

		// Parse join type
		joinType := p.parseJoinType()

		// Parse right table factor
		right, err := p.parseTableFactor()
		if err != nil {
			return nil, err
		}

		// Parse ON condition
		if !p.expectPeek(lexer.ON) {
			return nil, fmt.Errorf("expected ON after joined table")
		}

		p.nextToken() // move to start of expression
		condition, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ON condition: %w", err)
		}

		// Combine into a Join node
		left = &Join{
			Left:      left,
			Right:     right,
			Type:      joinType,
			Condition: condition,
		}
	}

	return left, nil
}

// parseTableFactor parses a single table or derived table with optional alias
func (p *Parser) parseTableFactor() (TableReference, error) {
	// Check for derived table: (SELECT ...)
	if p.peekIs(lexer.LPAREN) {
		p.nextToken() // consume (

		if p.peekIs(lexer.SELECT) {
			p.nextToken() // consume SELECT
			p.nextToken() // moves to first column (or *)
			subquery, err := p.parseSelectBody()
			if err != nil {
				return nil, err
			}

			if !p.expectPeek(lexer.RPAREN) {
				return nil, fmt.Errorf("expected ')' after derived table")
			}

			derivedTable := &DerivedTable{Subquery: subquery}

			// Parse alias (optional but highly recommended for derived tables)
			if p.peekIs(lexer.AS_KW) {
				p.nextToken() // AS
				if !p.expectPeek(lexer.IDENT) {
					return nil, fmt.Errorf("expected alias after AS")
				}
				derivedTable.Alias = p.cur.Literal
			} else if p.peekIs(lexer.IDENT) {
				p.nextToken()
				derivedTable.Alias = p.cur.Literal
			}

			return derivedTable, nil
		}

		// If we are here, it might be a nested join or just parenthesized table (not implemented yet)
		// For now, fall through or error
		return nil, fmt.Errorf("expected SELECT after '(' in table reference")
	}

	// Regular table or table function
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}

	name := p.cur.Literal

	// Check if this is a table function call: func_name(args)
	if p.peekIs(lexer.LPAREN) {
		p.nextToken() // consume (

		// Parse arguments
		var args []Expression
		if !p.peekIs(lexer.RPAREN) {
			p.nextToken() // move to first argument
			for {
				arg, err := p.parseExpression(LOWEST)
				if err != nil {
					return nil, fmt.Errorf("failed to parse table function argument: %w", err)
				}
				args = append(args, arg)

				if !p.peekIs(lexer.COMMA) {
					break
				}
				p.nextToken() // consume ,
				p.nextToken() // move to next argument
			}
		}

		if !p.expectPeek(lexer.RPAREN) {
			return nil, fmt.Errorf("expected ')' after table function arguments")
		}

		tableFunc := &TableFunction{Name: name, Args: args}

		// Parse alias
		if p.peekIs(lexer.AS_KW) {
			p.nextToken() // AS
			if !p.expectPeek(lexer.IDENT) {
				return nil, fmt.Errorf("expected alias after AS")
			}
			tableFunc.Alias = p.cur.Literal
		} else if p.peekIs(lexer.IDENT) {
			p.nextToken()
			tableFunc.Alias = p.cur.Literal
		}

		return tableFunc, nil
	}

	table := &Table{Name: name}

	// Parse alias
	if p.peekIs(lexer.AS_KW) {
		p.nextToken() // AS
		if !p.expectPeek(lexer.IDENT) {
			return nil, fmt.Errorf("expected alias after AS")
		}
		table.Alias = p.cur.Literal
	} else if p.peekIs(lexer.IDENT) {
		p.nextToken()
		table.Alias = p.cur.Literal
	}

	return table, nil
}

// isJoinStart checks if the peek token starts a JOIN clause
func (p *Parser) isJoinStart() bool {
	t := p.peek.Type
	return t == lexer.JOIN || t == lexer.INNER || t == lexer.LEFT || t == lexer.RIGHT || t == lexer.FULL || t == lexer.OUTER
}

// parseJoinType consumes tokens and returns the JoinType
func (p *Parser) parseJoinType() JoinType {
	p.nextToken() // Move to the first token of the join (e.g. JOIN, LEFT, INNER)

	switch p.cur.Type {
	case lexer.JOIN:
		return JoinInner
	case lexer.INNER:
		if p.peekIs(lexer.JOIN) {
			p.nextToken()
		}
		return JoinInner
	case lexer.LEFT:
		if p.peekIs(lexer.OUTER) {
			p.nextToken()
		}
		if p.peekIs(lexer.JOIN) {
			p.nextToken()
		}
		return JoinLeft
	case lexer.RIGHT:
		if p.peekIs(lexer.OUTER) {
			p.nextToken()
		}
		if p.peekIs(lexer.JOIN) {
			p.nextToken()
		}
		return JoinRight
	case lexer.FULL:
		if p.peekIs(lexer.OUTER) {
			p.nextToken()
		}
		if p.peekIs(lexer.JOIN) {
			p.nextToken()
		}
		return JoinFull
	case lexer.OUTER:
		// Implicit LEFT OUTER? No, usually FULL or error, but let's assume syntax error if not preceded by LEFT/RIGHT/FULL.
		// But if we just see OUTER JOIN? SQLite treats as ...?
		// Minimal standard SQL usually needs LEFT/RIGHT/FULL.
		// Let's assume syntax error if just OUTER, but to be safe consume JOIN if present.
		if p.peekIs(lexer.JOIN) {
			p.nextToken()
		}
		return JoinLeft // Fallback
	default:
		return JoinInner
	}
}

// parseSelectColumns parses: * | column, column, ... | function(args), ...
func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	if p.cur.Type == lexer.STAR {
		cols = append(cols, SelectColumn{Star: true})
		return cols, nil
	}

	for {
		expr, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}

		col := SelectColumn{Expr: expr}

		// Check for Alias
		if p.peekIs(lexer.AS_KW) {
			p.nextToken() // AS
			if !p.expectPeek(lexer.IDENT) {
				return nil, fmt.Errorf("expected alias after AS, got %s", p.peek.Literal)
			}
			col.Alias = p.cur.Literal
		} else if p.peekIs(lexer.IDENT) {
			// Optional alias without AS
			// But check if it's a keyword that starts a clause (FROM, WHERE, etc.)
			// Note: FROM is not in the keywords map as a reserved word?
			// Wait, FROM is a token type.
			// If p.peek is FROM, it won't be IDENT type if it is lexed as FROM token.
			// So checking for IDENT is safe assuming FROM is lexed as FROM.
			p.nextToken()
			col.Alias = p.cur.Literal
		}

		cols = append(cols, col)

		if p.peekIs(lexer.COMMA) {
			p.nextToken() // consume comma
			p.nextToken() // move to next expression start
		} else {
			break
		}
	}

	return cols, nil
}

// parseDropTableBody parses: TABLE [IF EXISTS] name [CASCADE]
// Called after DROP has been consumed and current token is TABLE
func (p *Parser) parseDropTableBody() (*DropTableStmt, error) {
	stmt := &DropTableStmt{}

	// Check for optional IF EXISTS
	if p.peekIs(lexer.IF) {
		p.nextToken() // consume TABLE, now at IF
		if !p.expectPeek(lexer.EXISTS) {
			return nil, fmt.Errorf("expected EXISTS after IF, got %s", p.peek.Literal)
		}
		stmt.IfExists = true
		// now at EXISTS, need to move to table name
	}

	// Current token is TABLE or EXISTS, move to table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// Check for optional CASCADE
	if p.peekIs(lexer.CASCADE) {
		p.nextToken() // move to CASCADE
		stmt.Cascade = true
	}

	return stmt, nil
}

// parseCreateIndex parses: INDEX name ON table (column | expr, ...) [WHERE expr]
// Supports both plain column indexes, expression indexes, and partial indexes.
// Called after CREATE [UNIQUE] INDEX has been consumed and current token is INDEX
func (p *Parser) parseCreateIndex(unique bool) (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{Unique: unique}

	// Current token is INDEX, move to index name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected index name, got %s", p.peek.Literal)
	}
	stmt.IndexName = p.cur.Literal

	// ON
	if !p.expectPeek(lexer.ON) {
		return nil, fmt.Errorf("expected ON, got %s", p.peek.Literal)
	}

	// Table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// (
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(', got %s", p.peek.Literal)
	}

	// Parse index elements (columns or expressions)
	columns, expressions, err := p.parseIndexElements()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns
	stmt.Expressions = expressions

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' or ',', got %s", p.peek.Literal)
	}

	// Optional WHERE clause for partial indexes
	if p.peekIs(lexer.WHERE) {
		p.nextToken() // consume WHERE
		p.nextToken() // move to first token of expression
		whereExpr, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %v", err)
		}
		stmt.Where = whereExpr
	}

	return stmt, nil
}

// parseIndexElements parses a list of index elements which can be:
// - Plain column name: name
// - Function call: UPPER(name), LOWER(email)
// - Parenthesized expression: (price * quantity)
// Returns separate lists for plain columns and expressions.
func (p *Parser) parseIndexElements() ([]string, []Expression, error) {
	var columns []string
	var expressions []Expression

	for {
		p.nextToken()

		// Check what kind of element this is
		if p.curIs(lexer.LPAREN) {
			// Parenthesized expression: ((price * quantity))
			// The outer paren is the index list delimiter, inner is for grouping
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return nil, nil, err
			}
			expressions = append(expressions, expr)
		} else if p.curIs(lexer.IDENT) {
			// Could be either:
			// 1. Plain column name: name
			// 2. Function call: UPPER(name)
			if p.peekIs(lexer.LPAREN) {
				// Function call - parse as expression
				expr, err := p.parseExpression(LOWEST)
				if err != nil {
					return nil, nil, err
				}
				expressions = append(expressions, expr)
			} else {
				// Plain column name
				columns = append(columns, p.cur.Literal)
			}
		} else {
			return nil, nil, fmt.Errorf("expected column name or expression, got %s", p.cur.Literal)
		}

		// Check for comma (more elements) or end
		if p.peekIs(lexer.COMMA) {
			p.nextToken() // consume comma
		} else if p.peekIs(lexer.RPAREN) {
			// End of list - don't consume, let caller handle it
			break
		} else {
			return nil, nil, fmt.Errorf("expected ',' or ')', got %s", p.peek.Literal)
		}
	}

	return columns, expressions, nil
}

// parseDropIndex parses: INDEX [IF EXISTS] name
// Called after DROP has been consumed and current token is INDEX
func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	stmt := &DropIndexStmt{}

	// Check for optional IF EXISTS
	if p.peekIs(lexer.IF) {
		p.nextToken() // consume INDEX, now at IF
		if !p.expectPeek(lexer.EXISTS) {
			return nil, fmt.Errorf("expected EXISTS after IF, got %s", p.peek.Literal)
		}
		stmt.IfExists = true
		// now at EXISTS, need to move to index name
	}

	// Current token is INDEX or EXISTS, move to index name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected index name, got %s", p.peek.Literal)
	}
	stmt.IndexName = p.cur.Literal

	return stmt, nil
}

// parseDropView parses: DROP VIEW [IF EXISTS] view_name
// Called after DROP VIEW has been consumed and current token is VIEW
func (p *Parser) parseDropView() (*DropViewStmt, error) {
	stmt := &DropViewStmt{}

	// Check for optional IF EXISTS
	if p.peekIs(lexer.IF) {
		p.nextToken() // consume VIEW, now at IF
		if !p.expectPeek(lexer.EXISTS) {
			return nil, fmt.Errorf("expected EXISTS after IF, got %s", p.peek.Literal)
		}
		stmt.IfExists = true
		// now at EXISTS, need to move to view name
	}

	// Current token is VIEW or EXISTS, move to view name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected view name, got %s", p.peek.Literal)
	}
	stmt.ViewName = p.cur.Literal

	return stmt, nil
}

// parseIdentList parses: ident, ident, ...
func (p *Parser) parseIdentList() ([]string, error) {
	var idents []string

	p.nextToken() // move to first ident
	for {
		if p.cur.Type != lexer.IDENT {
			return nil, fmt.Errorf("expected identifier, got %s", p.cur.Literal)
		}
		idents = append(idents, p.cur.Literal)

		if p.peekIs(lexer.COMMA) {
			p.nextToken() // ,
			p.nextToken() // next ident
		} else {
			break
		}
	}

	return idents, nil
}

// parseExpressionList parses: expr, expr, ...
func (p *Parser) parseExpressionList() ([]Expression, error) {
	var exprs []Expression

	p.nextToken() // move to first expr
	for {
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		if p.peekIs(lexer.COMMA) {
			p.nextToken() // ,
			p.nextToken() // next expr
		} else {
			break
		}
	}

	return exprs, nil
}

// Precedence levels for operators
const (
	_ int = iota
	LOWEST
	OR_PREC  // OR
	AND_PREC // AND
	IN_PREC  // IN, NOT IN
	EQUALS   // =, !=, <>, <, >, <=, >=
	SUM      // +, -
	PRODUCT  // *, /
	PREFIX   // -X, NOT
	CALL     // . (method call or property access)
)

// precedences maps token types to precedence
var precedences = map[lexer.TokenType]int{
	lexer.OR:    OR_PREC,
	lexer.AND:   AND_PREC,
	lexer.IN_KW: IN_PREC,
	lexer.EQ:    EQUALS,
	lexer.NEQ:   EQUALS,
	lexer.LT:    EQUALS,
	lexer.GT:    EQUALS,
	lexer.LTE:   EQUALS,
	lexer.GTE:   EQUALS,
	lexer.PLUS:  SUM,
	lexer.MINUS: SUM,
	lexer.STAR:  PRODUCT,
	lexer.SLASH: PRODUCT,
	lexer.DOT:   CALL,
}

// parseExpression parses an expression using Pratt parsing
func (p *Parser) parseExpression(precedence int) (Expression, error) {
	// Parse prefix
	left, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}

	// Parse infix
	// Stop on statement terminators and clause keywords
	for !p.peekIs(lexer.EOF) && !p.peekIs(lexer.SEMICOLON) && !p.peekIs(lexer.RPAREN) &&
		!p.peekIs(lexer.COMMA) && !p.peekIs(lexer.ASC) && !p.peekIs(lexer.DESC) &&
		!p.peekIs(lexer.ORDER) && !p.peekIs(lexer.LIMIT) && !p.peekIs(lexer.OFFSET) &&
		!p.peekIs(lexer.GROUP) && !p.peekIs(lexer.HAVING) &&
		// Stop on CASE expression keywords
		!p.peekIs(lexer.WHEN) && !p.peekIs(lexer.THEN) && !p.peekIs(lexer.ELSE_KW) && !p.peekIs(lexer.END) &&
		precedence < p.peekPrecedence() {
		p.nextToken()
		left, err = p.parseInfixExpression(left)
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

// parsePrefixExpression parses a prefix expression (literal, identifier, unary)
func (p *Parser) parsePrefixExpression() (Expression, error) {
	switch p.cur.Type {
	case lexer.INT:
		return p.parseIntLiteral()
	case lexer.FLOAT:
		return p.parseFloatLiteral()
	case lexer.STRING:
		return &Literal{Value: types.NewText(p.cur.Literal)}, nil
	case lexer.BLOB:
		return p.parseBlobLiteral()
	case lexer.NULL_KW:
		return &Literal{Value: types.NewNull()}, nil
	case lexer.TRUE_KW:
		return &Literal{Value: types.NewInt(1)}, nil
	case lexer.FALSE_KW:
		return &Literal{Value: types.NewInt(0)}, nil
	case lexer.RAISE:
		return p.parseRaiseExpression()
	case lexer.EXISTS:
		// EXISTS (SELECT ...)
		return p.parseExistsExpression(false)
	case lexer.CASE:
		return p.parseCaseExpression()
	case lexer.NOT:
		// Could be NOT EXISTS or NOT followed by expression
		if p.peekIs(lexer.EXISTS) {
			p.nextToken() // consume NOT, move to EXISTS
			return p.parseExistsExpression(true)
		}
		// NOT followed by expression
		op := p.cur.Type
		p.nextToken()
		right, err := p.parsePrefixExpression()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: op, Right: right}, nil
	case lexer.VALUES:
		// Handle VALUES(column) function
		if p.peekIs(lexer.LPAREN) {
			return p.parseFunctionCall()
		}
		return nil, fmt.Errorf("VALUES must be followed by '('")
	case lexer.IDENT:
		// Check if this is a function call (IDENT followed by LPAREN)
		if p.peekIs(lexer.LPAREN) {
			return p.parseFunctionCall()
		}
		return &ColumnRef{Name: p.cur.Literal}, nil
	case lexer.MINUS:
		op := p.cur.Type
		p.nextToken()
		right, err := p.parsePrefixExpression()
		if err != nil {
			return nil, err
		}
		// For literals, fold the negative sign
		if lit, ok := right.(*Literal); ok {
			if lit.Value.Type() == types.TypeInt {
				return &Literal{Value: types.NewInt(-lit.Value.Int())}, nil
			}
			if lit.Value.Type() == types.TypeFloat {
				return &Literal{Value: types.NewFloat(-lit.Value.Float())}, nil
			}
		}
		return &UnaryExpr{Op: op, Right: right}, nil
	case lexer.LPAREN:
		p.nextToken() // consume (
		// Check if this is a subquery: (SELECT ...)
		if p.cur.Type == lexer.SELECT {
			p.nextToken() // consume SELECT
			selectStmt, err := p.parseSelectBody()
			if err != nil {
				return nil, err
			}
			if !p.expectPeek(lexer.RPAREN) {
				return nil, fmt.Errorf("expected ')' after subquery, got %s", p.peek.Literal)
			}
			return &SubqueryExpr{Query: selectStmt}, nil
		}
		// Regular parenthesized expression
		expr, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}
		if !p.expectPeek(lexer.RPAREN) {
			return nil, fmt.Errorf("expected ')', got %s", p.peek.Literal)
		}
		return expr, nil
	default:
		return nil, fmt.Errorf("unexpected token in expression: %s", p.cur.Literal)
	}
}

// parseFunctionCall parses a function call: name(arg1, arg2, ...)
// Handles special cases like COUNT(*) where * is allowed as an argument
// Also handles window functions: func() OVER (...)
// Also handles window functions with OVER clause: func(args) OVER (...)
// Also handles window functions: func() OVER (...)
func (p *Parser) parseFunctionCall() (Expression, error) {
	funcCall := &FunctionCall{
		Name: p.cur.Literal,
	}

	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(' after function name")
	}

	// Handle empty args: func()
	if p.peekIs(lexer.RPAREN) {
		p.nextToken()
		return p.maybeParseWindowFunction(funcCall)
	}

	// Handle COUNT(*) special case
	if p.peekIs(lexer.STAR) {
		p.nextToken() // consume *
		// For COUNT(*), we use a special representation
		funcCall.Args = append(funcCall.Args, &Literal{Value: types.NewText("*")})
		if !p.expectPeek(lexer.RPAREN) {
			return nil, fmt.Errorf("expected ')' after '*'")
		}
		return p.maybeParseWindowFunction(funcCall)
	}

	// Parse argument list
	p.nextToken() // move to first argument
	for {
		arg, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}
		funcCall.Args = append(funcCall.Args, arg)

		if !p.peekIs(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma
		p.nextToken() // move to next argument
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' or ',' in function call")
	}

	// Special handling for VALUES(column) function
	if strings.ToUpper(funcCall.Name) == "VALUES" {
		if len(funcCall.Args) != 1 {
			return nil, fmt.Errorf("VALUES() function requires exactly one argument")
		}
		colRef, ok := funcCall.Args[0].(*ColumnRef)
		if !ok {
			return nil, fmt.Errorf("VALUES() argument must be a column name")
		}
		return &ValuesFunc{ColumnName: colRef.Name}, nil
	}

	return p.maybeParseWindowFunction(funcCall)
}

// maybeParseWindowFunction checks if the function call is followed by OVER clause
// and converts it to a WindowFunction if so
func (p *Parser) maybeParseWindowFunction(funcCall *FunctionCall) (Expression, error) {
	// Check if followed by OVER keyword
	if !p.peekIs(lexer.OVER) {
		return funcCall, nil
	}

	return p.parseWindowFunction(funcCall)
}

// parseWindowSpec parses a window specification: (PARTITION BY ... ORDER BY ...)
func (p *Parser) parseWindowSpec() (*WindowSpec, error) {
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(' after OVER")
	}

	spec := &WindowSpec{}

	// Handle empty window spec: OVER ()
	if p.peekIs(lexer.RPAREN) {
		p.nextToken()
		return spec, nil
	}

	p.nextToken() // move to first token in window spec

	// Parse PARTITION BY clause (optional)
	if p.cur.Type == lexer.PARTITION {
		if !p.expectPeek(lexer.BY) {
			return nil, fmt.Errorf("expected BY after PARTITION")
		}
		p.nextToken() // move to first partition expression

		for {
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return nil, err
			}
			spec.PartitionBy = append(spec.PartitionBy, expr)

			if !p.peekIs(lexer.COMMA) {
				break
			}
			p.nextToken() // consume comma
			p.nextToken() // move to next expression
		}

		// Check if there's an ORDER BY clause after
		if p.peekIs(lexer.ORDER) {
			p.nextToken() // consume ORDER
		}
	}

	// Parse ORDER BY clause (optional)
	if p.cur.Type == lexer.ORDER {
		if !p.expectPeek(lexer.BY) {
			return nil, fmt.Errorf("expected BY after ORDER in window spec")
		}
		p.nextToken() // move to first order expression

		for {
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return nil, err
			}

			direction := OrderAsc // Default is ASC
			if p.peekIs(lexer.ASC) {
				p.nextToken()
			} else if p.peekIs(lexer.DESC) {
				p.nextToken()
				direction = OrderDesc
			}

			spec.OrderBy = append(spec.OrderBy, OrderByExpr{
				Expr:      expr,
				Direction: direction,
			})

			if !p.peekIs(lexer.COMMA) {
				break
			}
			p.nextToken() // consume comma
			p.nextToken() // move to next expression
		}
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' at end of window specification")
	}

	return spec, nil
}

// parseWindowFunction parses the OVER clause and wraps the function in a WindowFunction
// Syntax: func(args) OVER ([PARTITION BY expr, ...] [ORDER BY expr [ASC|DESC], ...])
func (p *Parser) parseWindowFunction(funcCall *FunctionCall) (Expression, error) {
	p.nextToken() // consume OVER

	windowFunc := &WindowFunction{
		Function: funcCall,
		Over:     &WindowSpec{},
	}

	// Expect opening paren
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(' after OVER")
	}

	// Handle empty OVER ()
	if p.peekIs(lexer.RPAREN) {
		p.nextToken()
		return windowFunc, nil
	}

	// Parse PARTITION BY clause
	if p.peekIs(lexer.PARTITION) {
		p.nextToken() // consume PARTITION
		if !p.expectPeek(lexer.BY) {
			return nil, fmt.Errorf("expected BY after PARTITION")
		}

		// Parse partition expressions
		p.nextToken() // move to first expression
		for {
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return nil, err
			}
			windowFunc.Over.PartitionBy = append(windowFunc.Over.PartitionBy, expr)

			if !p.peekIs(lexer.COMMA) {
				break
			}
			// Check if next is ORDER BY (not another partition expression)
			if p.peekIs(lexer.ORDER) {
				break
			}
			p.nextToken() // consume comma
			p.nextToken() // move to next expression
		}
	}

	// Parse ORDER BY clause
	if p.peekIs(lexer.ORDER) {
		p.nextToken() // consume ORDER
		if !p.expectPeek(lexer.BY) {
			return nil, fmt.Errorf("expected BY after ORDER")
		}

		// Parse order expressions
		p.nextToken() // move to first expression
		for {
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return nil, err
			}

			direction := OrderAsc // default
			if p.peekIs(lexer.ASC) {
				p.nextToken()
			} else if p.peekIs(lexer.DESC) {
				p.nextToken()
				direction = OrderDesc
			}

			windowFunc.Over.OrderBy = append(windowFunc.Over.OrderBy, OrderByExpr{
				Expr:      expr,
				Direction: direction,
			})

			if !p.peekIs(lexer.COMMA) {
				break
			}
			// Check if next is ROWS/RANGE (not another order expression)
			if p.peekIs(lexer.ROWS) || p.peekIs(lexer.RANGE_KW) {
				break
			}
			p.nextToken() // consume comma
			p.nextToken() // move to next expression
		}
	}

	// Parse window frame clause (ROWS/RANGE BETWEEN ...)
	if p.peekIs(lexer.ROWS) || p.peekIs(lexer.RANGE_KW) {
		frame, err := p.parseWindowFrame()
		if err != nil {
			return nil, err
		}
		windowFunc.Over.Frame = frame
	}

	// Expect closing paren
	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' after window specification")
	}

	return windowFunc, nil
}

// parseWindowFrame parses a window frame specification
// Syntax: ROWS|RANGE BETWEEN <start_bound> AND <end_bound>
// <bound> ::= UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING | CURRENT ROW | <n> PRECEDING | <n> FOLLOWING
func (p *Parser) parseWindowFrame() (*WindowFrame, error) {
	frame := &WindowFrame{}

	// Parse mode: ROWS or RANGE
	p.nextToken() // move to ROWS or RANGE
	switch p.cur.Type {
	case lexer.ROWS:
		frame.Mode = FrameModeRows
	case lexer.RANGE_KW:
		frame.Mode = FrameModeRange
	default:
		return nil, fmt.Errorf("expected ROWS or RANGE, got %s", p.cur.Literal)
	}

	// Expect BETWEEN
	if !p.expectPeek(lexer.BETWEEN) {
		return nil, fmt.Errorf("expected BETWEEN after %s", p.cur.Literal)
	}

	// Parse start bound
	p.nextToken() // move past BETWEEN
	startBound, err := p.parseFrameBound()
	if err != nil {
		return nil, fmt.Errorf("invalid start bound: %w", err)
	}
	frame.StartBound = startBound

	// Expect AND
	if !p.expectPeek(lexer.AND) {
		return nil, fmt.Errorf("expected AND after start bound")
	}

	// Parse end bound
	p.nextToken() // move past AND
	endBound, err := p.parseFrameBound()
	if err != nil {
		return nil, fmt.Errorf("invalid end bound: %w", err)
	}
	frame.EndBound = endBound

	return frame, nil
}

// parseFrameBound parses a frame boundary
// <bound> ::= UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING | CURRENT ROW | <n> PRECEDING | <n> FOLLOWING
func (p *Parser) parseFrameBound() (*FrameBound, error) {
	bound := &FrameBound{}

	switch p.cur.Type {
	case lexer.UNBOUNDED:
		// UNBOUNDED PRECEDING or UNBOUNDED FOLLOWING
		p.nextToken() // move to PRECEDING or FOLLOWING
		if p.cur.Type == lexer.PRECEDING {
			bound.Type = FrameBoundUnboundedPreceding
		} else if p.cur.Type == lexer.FOLLOWING {
			bound.Type = FrameBoundUnboundedFollowing
		} else {
			return nil, fmt.Errorf("expected PRECEDING or FOLLOWING after UNBOUNDED, got %s", p.cur.Literal)
		}

	case lexer.CURRENT:
		// CURRENT ROW
		if !p.expectPeek(lexer.ROW) {
			return nil, fmt.Errorf("expected ROW after CURRENT")
		}
		bound.Type = FrameBoundCurrentRow

	case lexer.INT:
		// <n> PRECEDING or <n> FOLLOWING
		val, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", p.cur.Literal)
		}
		bound.Offset = &Literal{Value: types.NewInt(val)}

		p.nextToken() // move to PRECEDING or FOLLOWING
		if p.cur.Type == lexer.PRECEDING {
			bound.Type = FrameBoundPreceding
		} else if p.cur.Type == lexer.FOLLOWING {
			bound.Type = FrameBoundFollowing
		} else {
			return nil, fmt.Errorf("expected PRECEDING or FOLLOWING after offset, got %s", p.cur.Literal)
		}

	default:
		return nil, fmt.Errorf("unexpected token in frame bound: %s", p.cur.Literal)
	}

	return bound, nil
}

// parseInfixExpression parses a binary expression
func (p *Parser) parseInfixExpression(left Expression) (Expression, error) {
	// Handle DOT (table.column) specially
	if p.cur.Type == lexer.DOT {
		p.nextToken() // consume DOT

		// Expect identifier after DOT
		if p.cur.Type != lexer.IDENT {
			return nil, fmt.Errorf("expected identifier after '.', got %s", p.cur.Literal)
		}

		rightName := p.cur.Literal

		// If left is ColumnRef, merge
		if colRef, ok := left.(*ColumnRef); ok {
			return &ColumnRef{Name: colRef.Name + "." + rightName}, nil
		}

		return nil, fmt.Errorf("expected identifier before '.', got %T", left)
	}

	// Handle IN expression: expr IN (...)
	if p.cur.Type == lexer.IN_KW {
		return p.parseInExpression(left, false)
	}

	// Handle NOT IN: expr NOT IN (...)
	if p.cur.Type == lexer.NOT && p.peekIs(lexer.IN_KW) {
		p.nextToken() // consume NOT, now on IN
		return p.parseInExpression(left, true)
	}

	expr := &BinaryExpr{
		Left: left,
		Op:   p.cur.Type,
	}

	prec := p.curPrecedence()
	p.nextToken()

	right, err := p.parseExpression(prec)
	if err != nil {
		return nil, err
	}
	expr.Right = right

	return expr, nil
}

// parseExistsExpression parses: EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)
func (p *Parser) parseExistsExpression(notExists bool) (Expression, error) {
	// Current token is EXISTS
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(' after EXISTS, got %s", p.peek.Literal)
	}

	// Move to SELECT
	if !p.expectPeek(lexer.SELECT) {
		return nil, fmt.Errorf("expected SELECT in EXISTS subquery, got %s", p.peek.Literal)
	}
	p.nextToken() // consume SELECT

	selectStmt, err := p.parseSelectBody()
	if err != nil {
		return nil, err
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' after EXISTS subquery, got %s", p.peek.Literal)
	}

	return &ExistsExpr{
		Not:      notExists,
		Subquery: selectStmt,
	}, nil
}

// parseCaseExpression parses CASE expressions:
// Searched form: CASE WHEN condition THEN result [WHEN ...] [ELSE result] END
// Simple form: CASE operand WHEN value THEN result [WHEN ...] [ELSE result] END
func (p *Parser) parseCaseExpression() (Expression, error) {
	caseExpr := &CaseExpr{}

	// Check if this is a simple CASE (with operand) or searched CASE
	// If next token is WHEN, it's a searched CASE
	// Otherwise, parse the operand for a simple CASE
	if !p.peekIs(lexer.WHEN) {
		p.nextToken() // move to operand
		operand, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, fmt.Errorf("error parsing CASE operand: %w", err)
		}
		caseExpr.Operand = operand
	}

	// Parse WHEN clauses
	for p.peekIs(lexer.WHEN) {
		p.nextToken() // consume WHEN
		p.nextToken() // move to condition/value

		whenClause := &WhenClause{}

		// Parse condition/value
		condition, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, fmt.Errorf("error parsing WHEN condition: %w", err)
		}
		whenClause.Condition = condition

		// Expect THEN
		if !p.expectPeek(lexer.THEN) {
			return nil, fmt.Errorf("expected THEN after WHEN condition, got %s", p.peek.Literal)
		}
		p.nextToken() // move to THEN result

		// Parse THEN result
		thenResult, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, fmt.Errorf("error parsing THEN result: %w", err)
		}
		whenClause.Then = thenResult

		caseExpr.Whens = append(caseExpr.Whens, whenClause)
	}

	// Must have at least one WHEN clause
	if len(caseExpr.Whens) == 0 {
		return nil, fmt.Errorf("CASE expression must have at least one WHEN clause")
	}

	// Parse optional ELSE clause
	if p.peekIs(lexer.ELSE_KW) {
		p.nextToken() // consume ELSE
		p.nextToken() // move to ELSE result

		elseResult, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, fmt.Errorf("error parsing ELSE result: %w", err)
		}
		caseExpr.Else = elseResult
	}

	// Expect END
	if !p.expectPeek(lexer.END) {
		return nil, fmt.Errorf("expected END to close CASE expression, got %s", p.peek.Literal)
	}

	return caseExpr, nil
}

// parseInExpression parses: expr IN (...) or expr NOT IN (...)
func (p *Parser) parseInExpression(left Expression, notIn bool) (Expression, error) {
	// Current token is IN
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(' after IN, got %s", p.peek.Literal)
	}

	inExpr := &InExpr{
		Left: left,
		Not:  notIn,
	}

	p.nextToken() // move past (

	// Check if this is a subquery: IN (SELECT ...)
	if p.cur.Type == lexer.SELECT {
		p.nextToken() // consume SELECT
		selectStmt, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}
		inExpr.Subquery = selectStmt
	} else {
		// Value list: IN (1, 2, 3)
		for {
			expr, err := p.parseExpression(LOWEST)
			if err != nil {
				return nil, err
			}
			inExpr.Values = append(inExpr.Values, expr)

			if p.peekIs(lexer.COMMA) {
				p.nextToken() // consume comma
				p.nextToken() // move to next value
			} else {
				break
			}
		}
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' after IN list, got %s", p.peek.Literal)
	}

	return inExpr, nil
}

// parseIntLiteral parses an integer literal
func (p *Parser) parseIntLiteral() (*Literal, error) {
	val, err := strconv.ParseInt(p.cur.Literal, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid integer: %s", p.cur.Literal)
	}
	return &Literal{Value: types.NewInt(val)}, nil
}

// parseFloatLiteral parses a float literal
func (p *Parser) parseFloatLiteral() (*Literal, error) {
	val, err := strconv.ParseFloat(p.cur.Literal, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid float: %s", p.cur.Literal)
	}
	return &Literal{Value: types.NewFloat(val)}, nil
}

// parseAnalyze parses an ANALYZE statement
// ANALYZE [table_or_index_name]
func (p *Parser) parseAnalyze() (*AnalyzeStmt, error) {
	stmt := &AnalyzeStmt{}

	p.nextToken() // consume ANALYZE

	// Check if there's a table/index name
	if p.cur.Type == lexer.IDENT {
		stmt.TableName = p.cur.Literal
	}
	// If EOF or semicolon, it's ANALYZE without a target (analyze all)

	return stmt, nil
}

// Helper functions

func (p *Parser) curIs(t lexer.TokenType) bool {
	return p.cur.Type == t
}

func (p *Parser) peekIs(t lexer.TokenType) bool {
	return p.peek.Type == t
}

func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekIs(t) {
		p.nextToken()
		return true
	}
	return false
}

func (p *Parser) peekPrecedence() int {
	if prec, ok := precedences[p.peek.Type]; ok {
		return prec
	}
	return LOWEST
}

func (p *Parser) curPrecedence() int {
	if prec, ok := precedences[p.cur.Type]; ok {
		return prec
	}
	return LOWEST
}

// parseAlter parses ALTER TABLE statements
func (p *Parser) parseAlter() (Statement, error) {
	p.nextToken() // consume ALTER

	if p.cur.Type != lexer.TABLE {
		return nil, fmt.Errorf("expected TABLE after ALTER, got %s", p.cur.Literal)
	}

	// Table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name after ALTER TABLE, got %s", p.peek.Literal)
	}
	tableName := p.cur.Literal

	p.nextToken() // move to action keyword

	switch p.cur.Type {
	case lexer.ADD:
		return p.parseAlterTableAddColumn(tableName)
	case lexer.DROP:
		return p.parseAlterTableDropColumn(tableName)
	case lexer.RENAME:
		return p.parseAlterTableRename(tableName)
	default:
		return nil, fmt.Errorf("expected ADD, DROP, or RENAME after table name, got %s", p.cur.Literal)
	}
}

// parseAlterTableAddColumn parses: ADD [COLUMN] column_name type [constraints]
func (p *Parser) parseAlterTableAddColumn(tableName string) (*AlterTableStmt, error) {
	stmt := &AlterTableStmt{
		TableName: tableName,
		Action:    AlterActionAddColumn,
	}

	// Optional COLUMN keyword
	if p.peekIs(lexer.COLUMN) {
		p.nextToken() // consume COLUMN
	}

	// Column definition (name type constraints)
	p.nextToken() // move to column name
	col, err := p.parseColumnDef()
	if err != nil {
		return nil, fmt.Errorf("parsing column definition: %v", err)
	}
	stmt.NewColumn = &col

	return stmt, nil
}

// parseAlterTableDropColumn parses: DROP [COLUMN] column_name
func (p *Parser) parseAlterTableDropColumn(tableName string) (*AlterTableStmt, error) {
	stmt := &AlterTableStmt{
		TableName: tableName,
		Action:    AlterActionDropColumn,
	}

	// Optional COLUMN keyword
	if p.peekIs(lexer.COLUMN) {
		p.nextToken() // consume COLUMN
	}

	// Column name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected column name after DROP, got %s", p.peek.Literal)
	}
	stmt.ColumnName = p.cur.Literal

	return stmt, nil
}

// parseAlterTableRename parses: RENAME TO new_table_name
func (p *Parser) parseAlterTableRename(tableName string) (*AlterTableStmt, error) {
	stmt := &AlterTableStmt{
		TableName: tableName,
		Action:    AlterActionRenameTable,
	}

	// TO keyword
	if !p.expectPeek(lexer.TO) {
		return nil, fmt.Errorf("expected TO after RENAME, got %s", p.peek.Literal)
	}

	// New table name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected new table name after RENAME TO, got %s", p.peek.Literal)
	}
	stmt.NewName = p.cur.Literal

	return stmt, nil
}

// parseBegin parses: BEGIN [TRANSACTION]
func (p *Parser) parseBegin() (*BeginStmt, error) {
	// consume BEGIN
	// Optional TRANSACTION keyword
	if p.peekIs(lexer.TRANSACTION) {
		p.nextToken()
	}
	return &BeginStmt{}, nil
}

// parseCommit parses: COMMIT [TRANSACTION]
func (p *Parser) parseCommit() (*CommitStmt, error) {
	// consume COMMIT
	// Optional TRANSACTION keyword
	if p.peekIs(lexer.TRANSACTION) {
		p.nextToken()
	}
	return &CommitStmt{}, nil
}

// parseRollback parses: ROLLBACK [TRANSACTION] or ROLLBACK TO [SAVEPOINT] name
func (p *Parser) parseRollback() (Statement, error) {
	// Check if this is ROLLBACK TO [SAVEPOINT] name
	if p.peekIs(lexer.TO) {
		p.nextToken() // consume TO
		// Optional SAVEPOINT keyword
		if p.peekIs(lexer.SAVEPOINT) {
			p.nextToken() // consume SAVEPOINT
		}
		// Expect savepoint name
		if !p.expectPeek(lexer.IDENT) {
			return nil, fmt.Errorf("expected savepoint name after ROLLBACK TO, got %s", p.peek.Literal)
		}
		return &RollbackToStmt{Name: p.cur.Literal}, nil
	}

	// Regular ROLLBACK [TRANSACTION]
	// Optional TRANSACTION keyword
	if p.peekIs(lexer.TRANSACTION) {
		p.nextToken()
	}
	return &RollbackStmt{}, nil
}

// parseSavepoint parses: SAVEPOINT savepoint_name
func (p *Parser) parseSavepoint() (*SavepointStmt, error) {
	// consume SAVEPOINT, expect identifier
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected savepoint name after SAVEPOINT, got %s", p.peek.Literal)
	}
	return &SavepointStmt{Name: p.cur.Literal}, nil
}

// parseRelease parses: RELEASE [SAVEPOINT] savepoint_name
func (p *Parser) parseRelease() (*ReleaseStmt, error) {
	// Optional SAVEPOINT keyword
	if p.peekIs(lexer.SAVEPOINT) {
		p.nextToken() // consume SAVEPOINT
	}
	// Expect savepoint name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected savepoint name after RELEASE, got %s", p.peek.Literal)
	}
	return &ReleaseStmt{Name: p.cur.Literal}, nil
}

// parseWith parses: WITH [RECURSIVE] cte_name AS (SELECT ...), ... SELECT ...
func (p *Parser) parseWith() (*SelectStmt, error) {
	withClause := &WithClause{}

	// Current token is WITH
	// Check for optional RECURSIVE keyword
	if p.peekIs(lexer.RECURSIVE) {
		p.nextToken() // consume RECURSIVE
		withClause.Recursive = true
	}

	// Parse CTEs: cte_name [(columns)] AS (SELECT ...)
	for {
		cte, err := p.parseCTE()
		if err != nil {
			return nil, err
		}
		withClause.CTEs = append(withClause.CTEs, cte)

		// Check for more CTEs
		if !p.peekIs(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	// Expect SELECT
	if !p.expectPeek(lexer.SELECT) {
		return nil, fmt.Errorf("expected SELECT after WITH clause, got %s", p.peek.Literal)
	}
	p.nextToken() // move to columns

	// Parse the main SELECT body
	stmt, err := p.parseSelectBody()
	if err != nil {
		return nil, err
	}

	stmt.With = withClause
	return stmt, nil
}

// parseCTE parses: cte_name [(columns)] AS (SELECT ...)
func (p *Parser) parseCTE() (CTE, error) {
	cte := CTE{}

	// CTE name
	if !p.expectPeek(lexer.IDENT) {
		return cte, fmt.Errorf("expected CTE name, got %s", p.peek.Literal)
	}
	cte.Name = p.cur.Literal

	// Optional column list: (col1, col2, ...)
	if p.peekIs(lexer.LPAREN) {
		p.nextToken() // consume (
		cols, err := p.parseIdentList()
		if err != nil {
			return cte, err
		}
		cte.Columns = cols
		if !p.expectPeek(lexer.RPAREN) {
			return cte, fmt.Errorf("expected ')' after CTE column list")
		}
	}

	// AS
	if !p.expectPeek(lexer.AS_KW) {
		return cte, fmt.Errorf("expected AS after CTE name, got %s", p.peek.Literal)
	}

	// (SELECT ...)
	if !p.expectPeek(lexer.LPAREN) {
		return cte, fmt.Errorf("expected '(' after AS, got %s", p.peek.Literal)
	}

	if !p.expectPeek(lexer.SELECT) {
		return cte, fmt.Errorf("expected SELECT in CTE, got %s", p.peek.Literal)
	}
	p.nextToken() // move to columns

	selectStmt, err := p.parseSelectBody()
	if err != nil {
		return cte, err
	}

	// Check for set operations (UNION ALL for recursive CTE)
	query, err := p.parseSetOperations(selectStmt)
	if err != nil {
		return cte, err
	}
	cte.Query = query

	if !p.expectPeek(lexer.RPAREN) {
		return cte, fmt.Errorf("expected ')' after CTE SELECT")
	}

	return cte, nil
}

// parseCreateView parses: VIEW [IF NOT EXISTS] view_name [(columns)] AS SELECT ...
// Called after CREATE [IF NOT EXISTS] VIEW has been consumed
func (p *Parser) parseCreateView(ifNotExists bool) (*CreateViewStmt, error) {
	stmt := &CreateViewStmt{IfNotExists: ifNotExists}

	// Check for IF NOT EXISTS when it comes after VIEW
	// e.g., CREATE VIEW IF NOT EXISTS my_view AS ...
	if p.peekIs(lexer.IF) {
		p.nextToken() // consume IF
		if !p.expectPeek(lexer.NOT) {
			return nil, fmt.Errorf("expected NOT after IF, got %s", p.peek.Literal)
		}
		if !p.expectPeek(lexer.EXISTS) {
			return nil, fmt.Errorf("expected EXISTS after NOT, got %s", p.peek.Literal)
		}
		stmt.IfNotExists = true
	}

	// View name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected view name, got %s", p.peek.Literal)
	}
	stmt.ViewName = p.cur.Literal

	// Optional column list: (col1, col2, ...)
	if p.peekIs(lexer.LPAREN) {
		p.nextToken() // consume (
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, fmt.Errorf("parsing view column list: %w", err)
		}
		stmt.Columns = cols
		if !p.expectPeek(lexer.RPAREN) {
			return nil, fmt.Errorf("expected ')' after view column list")
		}
	}

	// AS keyword
	if !p.expectPeek(lexer.AS_KW) {
		return nil, fmt.Errorf("expected AS after view name, got %s", p.peek.Literal)
	}

	// SELECT statement
	if !p.expectPeek(lexer.SELECT) {
		return nil, fmt.Errorf("expected SELECT after AS, got %s", p.peek.Literal)
	}
	p.nextToken() // move to columns

	selectStmt, err := p.parseSelectBody()
	if err != nil {
		return nil, fmt.Errorf("parsing view SELECT: %w", err)
	}
	stmt.Query = selectStmt

	return stmt, nil
}

// parseExplain parses EXPLAIN and EXPLAIN QUERY PLAN statements
func (p *Parser) parseExplain() (*ExplainStmt, error) {
	stmt := &ExplainStmt{}

	p.nextToken() // consume EXPLAIN

	// Check for ANALYZE
	if p.curIs(lexer.ANALYZE) {
		stmt.Analyze = true
		p.nextToken() // consume ANALYZE
	}

	// Check for QUERY PLAN
	if p.curIs(lexer.QUERY) {
		if !p.expectPeek(lexer.PLAN) {
			return nil, fmt.Errorf("expected PLAN after QUERY, got %s", p.peek.Literal)
		}
		stmt.QueryPlan = true
		p.nextToken() // consume PLAN
	}

	// Parse the statement to explain
	innerStmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parsing statement in EXPLAIN: %w", err)
	}
	stmt.Statement = innerStmt

	return stmt, nil
}

// parseCreateTrigger parses: CREATE TRIGGER name BEFORE|AFTER INSERT|UPDATE|DELETE ON table BEGIN actions END
// Called after CREATE TRIGGER has been consumed and current token is TRIGGER
func (p *Parser) parseCreateTrigger() (*CreateTriggerStmt, error) {
	stmt := &CreateTriggerStmt{}

	// Trigger name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected trigger name after TRIGGER, got %s", p.peek.Literal)
	}
	stmt.TriggerName = p.cur.Literal

	// BEFORE or AFTER
	p.nextToken()
	switch p.cur.Type {
	case lexer.BEFORE:
		stmt.Timing = TriggerBefore
	case lexer.AFTER:
		stmt.Timing = TriggerAfter
	default:
		return nil, fmt.Errorf("expected BEFORE or AFTER, got %s", p.cur.Literal)
	}

	// INSERT, UPDATE, or DELETE
	p.nextToken()
	switch p.cur.Type {
	case lexer.INSERT:
		stmt.Event = TriggerEventInsert
	case lexer.UPDATE:
		stmt.Event = TriggerEventUpdate
	case lexer.DELETE:
		stmt.Event = TriggerEventDelete
	default:
		return nil, fmt.Errorf("expected INSERT, UPDATE, or DELETE, got %s", p.cur.Literal)
	}

	// ON table_name
	if !p.expectPeek(lexer.ON) {
		return nil, fmt.Errorf("expected ON after event type, got %s", p.peek.Literal)
	}
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name after ON, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// BEGIN
	if !p.expectPeek(lexer.BEGIN) {
		return nil, fmt.Errorf("expected BEGIN, got %s", p.peek.Literal)
	}

	// Parse action statements until END
	for {
		p.nextToken()

		// Check for END
		if p.cur.Type == lexer.END {
			break
		}

		// Parse each action statement
		action, err := p.Parse()
		if err != nil {
			return nil, fmt.Errorf("parsing trigger action: %w", err)
		}
		stmt.Actions = append(stmt.Actions, action)

		// Skip optional semicolon after each statement
		if p.peekIs(lexer.SEMICOLON) {
			p.nextToken()
		}
	}

	if len(stmt.Actions) == 0 {
		return nil, fmt.Errorf("trigger must have at least one action statement")
	}

	return stmt, nil
}

// parseDropTrigger parses: DROP TRIGGER [IF EXISTS] name
// Called after DROP TRIGGER has been consumed and current token is TRIGGER
func (p *Parser) parseDropTrigger() (*DropTriggerStmt, error) {
	stmt := &DropTriggerStmt{}

	// Check for optional IF EXISTS
	if p.peekIs(lexer.IF) {
		p.nextToken() // consume TRIGGER, now at IF
		if !p.expectPeek(lexer.EXISTS) {
			return nil, fmt.Errorf("expected EXISTS after IF, got %s", p.peek.Literal)
		}
		stmt.IfExists = true
	}

	// Trigger name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected trigger name, got %s", p.peek.Literal)
	}
	stmt.TriggerName = p.cur.Literal

	return stmt, nil
}

// parseRaiseExpression parses: RAISE(ABORT, 'message') or RAISE(IGNORE)
func (p *Parser) parseRaiseExpression() (*RaiseExpr, error) {
	// Current token is RAISE
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(' after RAISE, got %s", p.peek.Literal)
	}

	p.nextToken() // move to ABORT or IGNORE

	raise := &RaiseExpr{}

	switch p.cur.Type {
	case lexer.ABORT:
		raise.Type = RaiseAbort
		// Expect comma and message
		if !p.expectPeek(lexer.COMMA) {
			return nil, fmt.Errorf("expected ',' after ABORT, got %s", p.peek.Literal)
		}
		if !p.expectPeek(lexer.STRING) {
			return nil, fmt.Errorf("expected error message string, got %s", p.peek.Literal)
		}
		raise.Message = p.cur.Literal
	case lexer.IGNORE:
		raise.Type = RaiseIgnore
		// No message for IGNORE
	default:
		return nil, fmt.Errorf("expected ABORT or IGNORE in RAISE, got %s", p.cur.Literal)
	}

	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' after RAISE arguments, got %s", p.peek.Literal)
	}

	return raise, nil
}

// parseIfStmt parses an IF...THEN...ELSIF...ELSE...END IF statement
// Syntax: IF condition THEN statements [ELSIF condition THEN statements]... [ELSE statements] END IF
func (p *Parser) parseIfStmt() (*IfStmt, error) {
	stmt := &IfStmt{}

	// Currently on IF token
	p.nextToken() // move to condition

	// Parse the IF condition
	cond, err := p.parseExpression(LOWEST)
	if err != nil {
		return nil, fmt.Errorf("parsing IF condition: %w", err)
	}
	stmt.Condition = cond

	// Expect THEN
	if !p.expectPeek(lexer.THEN) {
		return nil, fmt.Errorf("expected THEN after IF condition, got %s", p.peek.Literal)
	}

	// Parse THEN branch statements
	thenStmts, err := p.parseIfBodyStatements()
	if err != nil {
		return nil, fmt.Errorf("parsing THEN branch: %w", err)
	}
	stmt.ThenBranch = thenStmts

	// After parseIfBodyStatements, we're positioned on ELSIF, ELSE, or END

	// Parse ELSIF clauses
	for p.curIs(lexer.ELSIF) || (p.curIs(lexer.IDENT) && (p.cur.Literal == "ELSIF" || p.cur.Literal == "ELSEIF")) {
		elsifClause, err := p.parseElsIfClause()
		if err != nil {
			return nil, err
		}
		stmt.ElsIfClauses = append(stmt.ElsIfClauses, elsifClause)
	}

	// Parse ELSE branch if present
	if p.curIs(lexer.ELSE_KW) {
		p.nextToken() // consume ELSE

		elseStmts, err := p.parseIfBodyStatements()
		if err != nil {
			return nil, fmt.Errorf("parsing ELSE branch: %w", err)
		}
		stmt.ElseBranch = elseStmts
	}

	// Expect END IF
	if !p.curIs(lexer.END) {
		return nil, fmt.Errorf("expected END at end of IF statement, got %s", p.cur.Literal)
	}
	p.nextToken() // consume END

	if !p.curIs(lexer.IF) {
		return nil, fmt.Errorf("expected IF after END, got %s", p.cur.Literal)
	}

	return stmt, nil
}

// parseElsIfClause parses an ELSIF condition THEN statements
func (p *Parser) parseElsIfClause() (*ElsIfClause, error) {
	clause := &ElsIfClause{}

	// Currently on ELSIF token
	p.nextToken() // move to condition

	// Parse condition
	cond, err := p.parseExpression(LOWEST)
	if err != nil {
		return nil, fmt.Errorf("parsing ELSIF condition: %w", err)
	}
	clause.Condition = cond

	// Expect THEN
	if !p.expectPeek(lexer.THEN) {
		return nil, fmt.Errorf("expected THEN after ELSIF condition, got %s", p.peek.Literal)
	}

	// Parse body statements
	stmts, err := p.parseIfBodyStatements()
	if err != nil {
		return nil, fmt.Errorf("parsing ELSIF body: %w", err)
	}
	clause.Body = stmts

	return clause, nil
}

// parseIfBodyStatements parses statements inside an IF/ELSIF/ELSE block
// Stops when it sees ELSIF, ELSE, or END
func (p *Parser) parseIfBodyStatements() ([]Statement, error) {
	var stmts []Statement

	p.nextToken() // move to first statement

	for !p.curIs(lexer.EOF) && !p.curIs(lexer.ELSIF) && !p.curIs(lexer.ELSE_KW) && !p.curIs(lexer.END) {
		// Check for ELSEIF as identifier (in case it's not a keyword)
		if p.curIs(lexer.IDENT) && (p.cur.Literal == "ELSIF" || p.cur.Literal == "ELSEIF") {
			break
		}

		// Parse statement at current position
		stmt, err := p.parseStatementAtCurrent()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, stmt)

		// Skip optional semicolon
		if p.peekIs(lexer.SEMICOLON) {
			p.nextToken()
			p.nextToken() // move past semicolon to next statement
		} else {
			p.nextToken() // move to next token
		}
	}

	return stmts, nil
}

// parseStatementAtCurrent parses a statement starting at the current token position
// This is similar to Parse() but doesn't advance past the first token
func (p *Parser) parseStatementAtCurrent() (Statement, error) {
	switch p.cur.Type {
	case lexer.SELECT:
		return p.parseSelect()
	case lexer.INSERT:
		return p.parseInsert()
	case lexer.UPDATE:
		return p.parseUpdate()
	case lexer.DELETE:
		return p.parseDelete()
	case lexer.IF:
		return p.parseIfStmt()
	default:
		return nil, fmt.Errorf("unexpected token in IF body: %s", p.cur.Literal)
	}
}
