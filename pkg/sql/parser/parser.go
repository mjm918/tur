// pkg/sql/parser/parser.go
package parser

import (
	"fmt"
	"strconv"

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
	case lexer.DROP:
		return p.parseDrop()
	case lexer.ANALYZE:
		return p.parseAnalyze()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.cur.Literal)
	}
}

// parseCreate handles CREATE TABLE and CREATE INDEX statements
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
	default:
		return nil, fmt.Errorf("expected TABLE, INDEX, or UNIQUE after CREATE, got %s", p.cur.Literal)
	}
}

// parseDrop handles DROP TABLE and DROP INDEX statements
func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // consume DROP

	switch p.cur.Type {
	case lexer.TABLE:
		return p.parseDropTableBody()
	case lexer.INDEX:
		return p.parseDropIndex()
	default:
		return nil, fmt.Errorf("expected TABLE or INDEX after DROP, got %s", p.cur.Literal)
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

	return stmt, nil
}

// parseSelect parses: SELECT columns FROM table [WHERE expr]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	// SELECT
	p.nextToken()

	return p.parseSelectBody()
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

	// From
	if !p.expectPeek(lexer.FROM) {
		return nil, fmt.Errorf("expected FROM, got %s", p.peek.Literal)
	}

	tableRef, err := p.parseTableReference()
	if err != nil {
		return nil, err
	}
	stmt.From = tableRef

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

	return stmt, nil
}

// parseTableReference parses: table [AS alias] [JOIN table ON ...]
func (p *Parser) parseTableReference() (TableReference, error) {
	// Parse left side (Table or subquery/nested join inside parens - for now just Table)
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}

	var left TableReference = &Table{Name: p.cur.Literal}

	// Check for Alias (optional AS identifier or just identifier)
	// TODO: Implement alias support properly if needed. For now sticking to simple table names.

	// Loop to handle multiple joins: t1 JOIN t2 JOIN t3 ... -> ((t1 JOIN t2) JOIN t3)
	for {
		if !p.isJoinStart() {
			break
		}

		// Parse join type
		joinType := p.parseJoinType()

		// Parse right table - expecting a table name for now
		if !p.expectPeek(lexer.IDENT) {
			return nil, fmt.Errorf("expected table name after JOIN, got %s", p.peek.Literal)
		}
		right := &Table{Name: p.cur.Literal}

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

// parseSelectColumns parses: * | column, column, ...
func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	if p.cur.Type == lexer.STAR {
		cols = append(cols, SelectColumn{Star: true})
		return cols, nil
	}

	for {
		if p.cur.Type != lexer.IDENT {
			return nil, fmt.Errorf("expected column name, got %s", p.cur.Literal)
		}
		name := p.cur.Literal

		// Handle qualified name (table.column)
		if p.peekIs(lexer.DOT) {
			p.nextToken() // move to DOT
			p.nextToken() // move to column name
			if p.cur.Type != lexer.IDENT {
				return nil, fmt.Errorf("expected column name after dot, got %s", p.cur.Literal)
			}
			name = name + "." + p.cur.Literal
		}

		cols = append(cols, SelectColumn{Name: name})

		if p.peekIs(lexer.COMMA) {
			p.nextToken() // ,
			p.nextToken() // next column
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

// parseCreateIndex parses: INDEX name ON table (column, ...)
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

	// Column names
	columns, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	// )
	if !p.expectPeek(lexer.RPAREN) {
		return nil, fmt.Errorf("expected ')' or ',', got %s", p.peek.Literal)
	}

	return stmt, nil
}

// parseDropIndex parses: INDEX name
// Called after DROP has been consumed and current token is INDEX
func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	stmt := &DropIndexStmt{}

	// Current token is INDEX, move to index name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected index name, got %s", p.peek.Literal)
	}
	stmt.IndexName = p.cur.Literal

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
	for !p.peekIs(lexer.EOF) && !p.peekIs(lexer.SEMICOLON) && !p.peekIs(lexer.RPAREN) &&
		!p.peekIs(lexer.COMMA) && precedence < p.peekPrecedence() {
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
	case lexer.IDENT:
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
		p.nextToken() // (
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
