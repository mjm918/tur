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
	BLOB   // x'1234'

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
	COLON     // :
	LPAREN    // (
	RPAREN    // )
	DOT       // .
	QUESTION  // ? (parameter placeholder)

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

	// Conditional keywords
	IF
	EXISTS

	// Join keywords
	JOIN
	INNER
	LEFT
	RIGHT
	FULL
	OUTER

	// Statistics keywords
	ANALYZE

	// ALTER keywords
	ALTER
	ADD
	COLUMN
	RENAME
	TO

	// Aggregation keywords
	GROUP
	HAVING

	// Subquery keywords
	IN_KW   // IN for subqueries
	AS_KW   // AS for aliases
	LIKE_KW // LIKE for pattern matching

	// Transaction keywords
	BEGIN
	COMMIT
	ROLLBACK
	TRANSACTION

	// Set operation keywords
	UNION
	INTERSECT
	EXCEPT
	ALL

	// CTE keywords
	WITH
	RECURSIVE

	// View keywords
	VIEW

	// Explain keywords
	EXPLAIN
	QUERY
	PLAN

	// Savepoint keywords
	SAVEPOINT
	RELEASE

	// Window function keywords
	OVER
	PARTITION
	ROWS
	RANGE_KW // RANGE is used for window frame mode
	BETWEEN
	UNBOUNDED
	PRECEDING
	FOLLOWING
	CURRENT
	ROW

	// Trigger keywords
	TRIGGER
	BEFORE
	AFTER
	END

	// RAISE function keywords
	RAISE
	ABORT
	IGNORE

	// Vector keywords
	NONORMALIZE

	// Upsert keywords
	DUPLICATE

	// CASE expression keywords
	CASE
	WHEN
	THEN
	ELSE_KW

	// Truncate keywords
	TRUNCATE

	// Procedural SQL keywords
	ELSIF
	ELSEIF

	// Stored Procedure keywords
	PROCEDURE
	CALL
	DECLARE
	CURSOR
	OPEN
	FETCH
	CLOSE
	HANDLER
	CONTINUE
	EXIT
	FOR_KW // FOR (distinct from FOR loop)
	LOOP
	LEAVE
	FOUND
	INOUT
	OUT
	SQLEXCEPTION
	SQLWARNING
	SQLSTATE
	AT // @ for session variables

	// JSON tokens
	JSON_TYPE_KW  // JSON type keyword
	ARROW         // -> for JSON extract
	DOUBLE_ARROW  // ->> for JSON extract unquote
	LBRACKET      // [ for array access
	RBRACKET      // ] for array access

	// PRAGMA keyword
	PRAGMA

	// Strict data type keywords
	SMALLINT_TYPE
	BIGINT_TYPE
	SERIAL_TYPE
	BIGSERIAL_TYPE
	GUID_TYPE
	UUID_TYPE
	DECIMAL_TYPE
	NUMERIC_TYPE
	VARCHAR_TYPE
	CHAR_TYPE
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
	case BLOB:
		return "BLOB"
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
	case COLON:
		return ":"
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
	case IF:
		return "IF"
	case EXISTS:
		return "EXISTS"
	case JOIN:
		return "JOIN"
	case INNER:
		return "INNER"
	case LEFT:
		return "LEFT"
	case RIGHT:
		return "RIGHT"
	case FULL:
		return "FULL"
	case OUTER:
		return "OUTER"
	case ANALYZE:
		return "ANALYZE"
	case ALTER:
		return "ALTER"
	case ADD:
		return "ADD"
	case COLUMN:
		return "COLUMN"
	case RENAME:
		return "RENAME"
	case TO:
		return "TO"
	case GROUP:
		return "GROUP"
	case HAVING:
		return "HAVING"
	case IN_KW:
		return "IN"
	case AS_KW:
		return "AS"
	case LIKE_KW:
		return "LIKE"
	case BEGIN:
		return "BEGIN"
	case COMMIT:
		return "COMMIT"
	case ROLLBACK:
		return "ROLLBACK"
	case TRANSACTION:
		return "TRANSACTION"
	case UNION:
		return "UNION"
	case INTERSECT:
		return "INTERSECT"
	case EXCEPT:
		return "EXCEPT"
	case ALL:
		return "ALL"
	case WITH:
		return "WITH"
	case RECURSIVE:
		return "RECURSIVE"
	case VIEW:
		return "VIEW"
	case EXPLAIN:
		return "EXPLAIN"
	case QUERY:
		return "QUERY"
	case PLAN:
		return "PLAN"
	case SAVEPOINT:
		return "SAVEPOINT"
	case RELEASE:
		return "RELEASE"
	case OVER:
		return "OVER"
	case PARTITION:
		return "PARTITION"
	case ROWS:
		return "ROWS"
	case RANGE_KW:
		return "RANGE"
	case BETWEEN:
		return "BETWEEN"
	case UNBOUNDED:
		return "UNBOUNDED"
	case PRECEDING:
		return "PRECEDING"
	case FOLLOWING:
		return "FOLLOWING"
	case CURRENT:
		return "CURRENT"
	case ROW:
		return "ROW"
	case TRIGGER:
		return "TRIGGER"
	case BEFORE:
		return "BEFORE"
	case AFTER:
		return "AFTER"
	case END:
		return "END"
	case RAISE:
		return "RAISE"
	case ABORT:
		return "ABORT"
	case IGNORE:
		return "IGNORE"
	case NONORMALIZE:
		return "NONORMALIZE"
	case DUPLICATE:
		return "DUPLICATE"
	case CASE:
		return "CASE"
	case WHEN:
		return "WHEN"
	case THEN:
		return "THEN"
	case ELSE_KW:
		return "ELSE"
	case TRUNCATE:
		return "TRUNCATE"
	case ELSIF:
		return "ELSIF"
	case ELSEIF:
		return "ELSEIF"
	case PROCEDURE:
		return "PROCEDURE"
	case CALL:
		return "CALL"
	case DECLARE:
		return "DECLARE"
	case CURSOR:
		return "CURSOR"
	case OPEN:
		return "OPEN"
	case FETCH:
		return "FETCH"
	case CLOSE:
		return "CLOSE"
	case HANDLER:
		return "HANDLER"
	case CONTINUE:
		return "CONTINUE"
	case EXIT:
		return "EXIT"
	case FOR_KW:
		return "FOR"
	case LOOP:
		return "LOOP"
	case LEAVE:
		return "LEAVE"
	case FOUND:
		return "FOUND"
	case INOUT:
		return "INOUT"
	case OUT:
		return "OUT"
	case SQLEXCEPTION:
		return "SQLEXCEPTION"
	case SQLWARNING:
		return "SQLWARNING"
	case SQLSTATE:
		return "SQLSTATE"
	case AT:
		return "@"
	case JSON_TYPE_KW:
		return "JSON"
	case ARROW:
		return "->"
	case DOUBLE_ARROW:
		return "->>"
	case LBRACKET:
		return "["
	case RBRACKET:
		return "]"
	case PRAGMA:
		return "PRAGMA"
	case SMALLINT_TYPE:
		return "SMALLINT"
	case BIGINT_TYPE:
		return "BIGINT"
	case SERIAL_TYPE:
		return "SERIAL"
	case BIGSERIAL_TYPE:
		return "BIGSERIAL"
	case GUID_TYPE:
		return "GUID"
	case UUID_TYPE:
		return "UUID"
	case DECIMAL_TYPE:
		return "DECIMAL"
	case NUMERIC_TYPE:
		return "NUMERIC"
	case VARCHAR_TYPE:
		return "VARCHAR"
	case CHAR_TYPE:
		return "CHAR"
	default:
		return "UNKNOWN"
	}
}

// keywords maps SQL keywords to their token types
var keywords = map[string]TokenType{
	"SELECT":      SELECT,
	"FROM":        FROM,
	"WHERE":       WHERE,
	"INSERT":      INSERT,
	"INTO":        INTO,
	"VALUES":      VALUES,
	"CREATE":      CREATE,
	"TABLE":       TABLE,
	"DROP":        DROP,
	"DELETE":      DELETE,
	"UPDATE":      UPDATE,
	"SET":         SET,
	"INT":         INT_TYPE,
	"TEXT":        TEXT_TYPE,
	"FLOAT":       FLOAT_TYPE,
	"REAL":        REAL,
	"BLOB":        BLOB_TYPE,
	"VECTOR":      VECTOR,
	"PRIMARY":     PRIMARY,
	"KEY":         KEY,
	"NOT":         NOT,
	"NULL":        NULL_KW,
	"AND":         AND,
	"OR":          OR,
	"TRUE":        TRUE_KW,
	"FALSE":       FALSE_KW,
	"ASC":         ASC,
	"DESC":        DESC,
	"ORDER":       ORDER,
	"BY":          BY,
	"LIMIT":       LIMIT,
	"OFFSET":      OFFSET,
	"UNIQUE":      UNIQUE,
	"CHECK":       CHECK,
	"DEFAULT":     DEFAULT,
	"FOREIGN":     FOREIGN,
	"REFERENCES":  REFERENCES,
	"ON":          ON,
	"CASCADE":     CASCADE,
	"RESTRICT":    RESTRICT,
	"ACTION":      ACTION,
	"CONSTRAINT":  CONSTRAINT,
	"INDEX":       INDEX,
	"IF":          IF,
	"EXISTS":      EXISTS,
	"JOIN":        JOIN,
	"INNER":       INNER,
	"LEFT":        LEFT,
	"RIGHT":       RIGHT,
	"FULL":        FULL,
	"OUTER":       OUTER,
	"ANALYZE":     ANALYZE,
	"ALTER":       ALTER,
	"ADD":         ADD,
	"COLUMN":      COLUMN,
	"RENAME":      RENAME,
	"TO":          TO,
	"GROUP":       GROUP,
	"UNION":       UNION,
	"INTERSECT":   INTERSECT,
	"EXCEPT":      EXCEPT,
	"ALL":         ALL,
	"HAVING":      HAVING,
	"IN":          IN_KW,
	"AS":          AS_KW,
	"LIKE":        LIKE_KW,
	"BEGIN":       BEGIN,
	"COMMIT":      COMMIT,
	"ROLLBACK":    ROLLBACK,
	"TRANSACTION": TRANSACTION,
	"WITH":        WITH,
	"RECURSIVE":   RECURSIVE,
	"VIEW":        VIEW,
	"SAVEPOINT":   SAVEPOINT,
	"RELEASE":     RELEASE,
	"EXPLAIN":     EXPLAIN,
	"QUERY":       QUERY,
	"PLAN":        PLAN,
	"OVER":        OVER,
	"PARTITION":   PARTITION,
	"ROWS":        ROWS,
	"RANGE":       RANGE_KW,
	"BETWEEN":     BETWEEN,
	"UNBOUNDED":   UNBOUNDED,
	"PRECEDING":   PRECEDING,
	"FOLLOWING":   FOLLOWING,
	"CURRENT":     CURRENT,
	"ROW":         ROW,
	"TRIGGER":     TRIGGER,
	"BEFORE":      BEFORE,
	"AFTER":       AFTER,
	"END":         END,
	"RAISE":       RAISE,
	"ABORT":       ABORT,
	"IGNORE":      IGNORE,
	"NONORMALIZE": NONORMALIZE,
	"DUPLICATE":   DUPLICATE,
	"CASE":        CASE,
	"WHEN":        WHEN,
	"THEN":        THEN,
	"ELSE":        ELSE_KW,
	"TRUNCATE":    TRUNCATE,
	"ELSIF":        ELSIF,
	"ELSEIF":       ELSEIF,
	"PROCEDURE":    PROCEDURE,
	"CALL":         CALL,
	"DECLARE":      DECLARE,
	"CURSOR":       CURSOR,
	"OPEN":         OPEN,
	"FETCH":        FETCH,
	"CLOSE":        CLOSE,
	"HANDLER":      HANDLER,
	"CONTINUE":     CONTINUE,
	"EXIT":         EXIT,
	"FOR":          FOR_KW,
	"LOOP":         LOOP,
	"LEAVE":        LEAVE,
	"FOUND":        FOUND,
	"INOUT":        INOUT,
	"OUT":          OUT,
	"SQLEXCEPTION": SQLEXCEPTION,
	"SQLWARNING":   SQLWARNING,
	"SQLSTATE":     SQLSTATE,
	"JSON":         JSON_TYPE_KW,
	"PRAGMA":       PRAGMA,
	"SMALLINT":     SMALLINT_TYPE,
	"BIGINT":       BIGINT_TYPE,
	"SERIAL":       SERIAL_TYPE,
	"BIGSERIAL":    BIGSERIAL_TYPE,
	"GUID":         GUID_TYPE,
	"UUID":         UUID_TYPE,
	"DECIMAL":      DECIMAL_TYPE,
	"NUMERIC":      NUMERIC_TYPE,
	"VARCHAR":      VARCHAR_TYPE,
	"CHAR":         CHAR_TYPE,
}

// LookupIdent checks if ident is a keyword, returns keyword token type or IDENT
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}
