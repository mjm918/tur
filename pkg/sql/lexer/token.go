// pkg/sql/lexer/token.go
package lexer

// TokenType represents the type of a lexical token
type TokenType int

const (
	EOF TokenType = iota
	ILLEGAL

	// Literals
	IDENT  // column_name, table_name
	INT    // 123
	FLOAT  // 1.23
	STRING // 'hello'

	// Operators
	PLUS  // +
	MINUS // -
	STAR  // *
	SLASH // /
	EQ    // =
	NEQ   // != or <>
	LT    // <
	GT    // >
	LTE   // <=
	GTE   // >=
	BANG  // !
	LTGT  // <> (alternate NEQ)

	// Delimiters
	COMMA     // ,
	SEMICOLON // ;
	LPAREN    // (
	RPAREN    // )
	DOT       // .

	// Keywords
	SELECT
	FROM
	WHERE
	INSERT
	INTO
	VALUES
	CREATE
	TABLE
	DROP
	DELETE
	UPDATE
	SET
	INT_TYPE
	INTEGER
	TEXT_TYPE
	FLOAT_TYPE
	REAL
	BLOB_TYPE
	VECTOR
	PRIMARY
	KEY
	NOT
	NULL_KW
	AND
	OR
	TRUE_KW
	FALSE_KW
	ASC
	DESC
	ORDER
	BY
	LIMIT
	OFFSET

	// Constraint keywords
	UNIQUE
	CHECK
	DEFAULT
	FOREIGN
	REFERENCES
	ON
	CASCADE
	RESTRICT
	ACTION
	CONSTRAINT

	// Index keywords
	INDEX
)

// Token represents a lexical token
type Token struct {
	Type    TokenType
	Literal string
	Pos     int // position in input
}

// String returns a string representation of the token type
func (t TokenType) String() string {
	switch t {
	case EOF:
		return "EOF"
	case ILLEGAL:
		return "ILLEGAL"
	case IDENT:
		return "IDENT"
	case INT:
		return "INT"
	case FLOAT:
		return "FLOAT"
	case STRING:
		return "STRING"
	case PLUS:
		return "+"
	case MINUS:
		return "-"
	case STAR:
		return "*"
	case SLASH:
		return "/"
	case EQ:
		return "="
	case NEQ:
		return "!="
	case LT:
		return "<"
	case GT:
		return ">"
	case LTE:
		return "<="
	case GTE:
		return ">="
	case COMMA:
		return ","
	case SEMICOLON:
		return ";"
	case LPAREN:
		return "("
	case RPAREN:
		return ")"
	case DOT:
		return "."
	case SELECT:
		return "SELECT"
	case FROM:
		return "FROM"
	case WHERE:
		return "WHERE"
	case INSERT:
		return "INSERT"
	case INTO:
		return "INTO"
	case VALUES:
		return "VALUES"
	case CREATE:
		return "CREATE"
	case TABLE:
		return "TABLE"
	case DROP:
		return "DROP"
	case DELETE:
		return "DELETE"
	case UPDATE:
		return "UPDATE"
	case SET:
		return "SET"
	case INT_TYPE:
		return "INT"
	case INTEGER:
		return "INTEGER"
	case TEXT_TYPE:
		return "TEXT"
	case FLOAT_TYPE:
		return "FLOAT"
	case REAL:
		return "REAL"
	case BLOB_TYPE:
		return "BLOB"
	case VECTOR:
		return "VECTOR"
	case PRIMARY:
		return "PRIMARY"
	case KEY:
		return "KEY"
	case NOT:
		return "NOT"
	case NULL_KW:
		return "NULL"
	case AND:
		return "AND"
	case OR:
		return "OR"
	case TRUE_KW:
		return "TRUE"
	case FALSE_KW:
		return "FALSE"
	case ASC:
		return "ASC"
	case DESC:
		return "DESC"
	case ORDER:
		return "ORDER"
	case BY:
		return "BY"
	case LIMIT:
		return "LIMIT"
	case OFFSET:
		return "OFFSET"
	case UNIQUE:
		return "UNIQUE"
	case CHECK:
		return "CHECK"
	case DEFAULT:
		return "DEFAULT"
	case FOREIGN:
		return "FOREIGN"
	case REFERENCES:
		return "REFERENCES"
	case ON:
		return "ON"
	case CASCADE:
		return "CASCADE"
	case RESTRICT:
		return "RESTRICT"
	case ACTION:
		return "ACTION"
	case CONSTRAINT:
		return "CONSTRAINT"
	case INDEX:
		return "INDEX"
	default:
		return "UNKNOWN"
	}
}

// keywords maps SQL keywords to their token types
var keywords = map[string]TokenType{
	"SELECT":  SELECT,
	"FROM":    FROM,
	"WHERE":   WHERE,
	"INSERT":  INSERT,
	"INTO":    INTO,
	"VALUES":  VALUES,
	"CREATE":  CREATE,
	"TABLE":   TABLE,
	"DROP":    DROP,
	"DELETE":  DELETE,
	"UPDATE":  UPDATE,
	"SET":     SET,
	"INT":     INT_TYPE,
	"INTEGER": INTEGER,
	"TEXT":    TEXT_TYPE,
	"FLOAT":   FLOAT_TYPE,
	"REAL":    REAL,
	"BLOB":    BLOB_TYPE,
	"VECTOR":  VECTOR,
	"PRIMARY": PRIMARY,
	"KEY":     KEY,
	"NOT":     NOT,
	"NULL":    NULL_KW,
	"AND":     AND,
	"OR":      OR,
	"TRUE":    TRUE_KW,
	"FALSE":   FALSE_KW,
	"ASC":     ASC,
	"DESC":    DESC,
	"ORDER":   ORDER,
	"BY":      BY,
	"LIMIT":      LIMIT,
	"OFFSET":     OFFSET,
	"UNIQUE":     UNIQUE,
	"CHECK":      CHECK,
	"DEFAULT":    DEFAULT,
	"FOREIGN":    FOREIGN,
	"REFERENCES": REFERENCES,
	"ON":         ON,
	"CASCADE":    CASCADE,
	"RESTRICT":   RESTRICT,
	"ACTION":     ACTION,
	"CONSTRAINT": CONSTRAINT,
	"INDEX":      INDEX,
}

// LookupIdent checks if ident is a keyword, returns keyword token type or IDENT
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}
