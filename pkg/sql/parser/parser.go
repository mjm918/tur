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
		return p.parseCreateTable()
	case lexer.INSERT:
		return p.parseInsert()
	case lexer.SELECT:
		return p.parseSelect()
	case lexer.DROP:
		return p.parseDropTable()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.cur.Literal)
	}
}

// parseCreateTable parses: CREATE TABLE name (column_def, ...)
func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}

	// CREATE
	if !p.expectPeek(lexer.TABLE) {
		return nil, fmt.Errorf("expected TABLE, got %s", p.peek.Literal)
	}

	// TABLE
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

	// (
	if !p.expectPeek(lexer.LPAREN) {
		return nil, fmt.Errorf("expected '(', got %s", p.peek.Literal)
	}

	// column definitions
	for {
		p.nextToken()
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)

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

// parseColumnDef parses: name TYPE [PRIMARY KEY] [NOT NULL]
func (p *Parser) parseColumnDef() (ColumnDef, error) {
	col := ColumnDef{}

	// Column name
	if p.cur.Type != lexer.IDENT {
		return col, fmt.Errorf("expected column name, got %s", p.cur.Literal)
	}
	col.Name = p.cur.Literal

	// Type
	p.nextToken()
	colType, err := p.parseColumnType()
	if err != nil {
		return col, err
	}
	col.Type = colType

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
		} else {
			break
		}
	}

	return col, nil
}

// parseColumnType parses a column type
func (p *Parser) parseColumnType() (types.ValueType, error) {
	switch p.cur.Type {
	case lexer.INT_TYPE, lexer.INTEGER:
		return types.TypeInt, nil
	case lexer.TEXT_TYPE:
		return types.TypeText, nil
	case lexer.FLOAT_TYPE, lexer.REAL:
		return types.TypeFloat, nil
	case lexer.BLOB_TYPE:
		return types.TypeBlob, nil
	case lexer.VECTOR:
		return types.TypeVector, nil
	default:
		return types.TypeNull, fmt.Errorf("expected type, got %s", p.cur.Literal)
	}
}

// parseInsert parses: INSERT INTO table [(columns)] VALUES (values), ...
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

	// VALUES
	if !p.expectPeek(lexer.VALUES) {
		return nil, fmt.Errorf("expected VALUES, got %s", p.peek.Literal)
	}

	// Value rows
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

	return stmt, nil
}

// parseSelect parses: SELECT columns FROM table [WHERE expr]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	// SELECT
	p.nextToken()

	// Columns
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if !p.expectPeek(lexer.FROM) {
		return nil, fmt.Errorf("expected FROM, got %s", p.peek.Literal)
	}

	// Table
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}
	stmt.From = p.cur.Literal

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
		cols = append(cols, SelectColumn{Name: p.cur.Literal})

		if p.peekIs(lexer.COMMA) {
			p.nextToken() // ,
			p.nextToken() // next column
		} else {
			break
		}
	}

	return cols, nil
}

// parseDropTable parses: DROP TABLE name
func (p *Parser) parseDropTable() (*DropTableStmt, error) {
	stmt := &DropTableStmt{}

	// DROP
	if !p.expectPeek(lexer.TABLE) {
		return nil, fmt.Errorf("expected TABLE, got %s", p.peek.Literal)
	}

	// TABLE name
	if !p.expectPeek(lexer.IDENT) {
		return nil, fmt.Errorf("expected table name, got %s", p.peek.Literal)
	}
	stmt.TableName = p.cur.Literal

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
