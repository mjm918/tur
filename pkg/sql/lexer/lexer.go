// pkg/sql/lexer/lexer.go
package lexer

import (
	"strings"
)

// Lexer tokenizes SQL input
type Lexer struct {
	input   string
	pos     int  // current position in input
	readPos int  // reading position (after current char)
	ch      byte // current char
}

// New creates a new Lexer for the given input
func New(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// readChar reads the next character
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

// peekChar returns the next character without advancing
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	var tok Token
	tok.Pos = l.pos

	switch l.ch {
	case '+':
		tok = l.newToken(PLUS, "+")
	case '-':
		tok = l.newToken(MINUS, "-")
	case '*':
		tok = l.newToken(STAR, "*")
	case '/':
		tok = l.newToken(SLASH, "/")
	case '=':
		tok = l.newToken(EQ, "=")
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: NEQ, Literal: "!=", Pos: tok.Pos}
		} else {
			tok = l.newToken(BANG, "!")
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: LTE, Literal: "<=", Pos: tok.Pos}
		} else if l.peekChar() == '>' {
			l.readChar()
			tok = Token{Type: NEQ, Literal: "<>", Pos: tok.Pos}
		} else {
			tok = l.newToken(LT, "<")
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: GTE, Literal: ">=", Pos: tok.Pos}
		} else {
			tok = l.newToken(GT, ">")
		}
	case ',':
		tok = l.newToken(COMMA, ",")
	case ';':
		tok = l.newToken(SEMICOLON, ";")
	case '(':
		tok = l.newToken(LPAREN, "(")
	case ')':
		tok = l.newToken(RPAREN, ")")
	case '.':
		if isDigit(l.peekChar()) {
			tok.Literal = l.readNumber()
			tok.Type = FLOAT
			return tok
		}
		tok = l.newToken(DOT, ".")
	case '\'':
		tok.Literal = l.readString()
		tok.Type = STRING
		return tok
	case 0:
		tok.Type = EOF
		tok.Literal = ""
		return tok
	default:
		if isLetter(l.ch) || l.ch == '_' {
			// Check for BLOB literal: x'...' or X'...'
			if (l.ch == 'x' || l.ch == 'X') && l.peekChar() == '\'' {
				l.readChar() // consume 'x'
				tok.Literal = l.readString()
				tok.Type = BLOB
				return tok
			}
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(strings.ToUpper(tok.Literal))
			return tok
		} else if isDigit(l.ch) {
			tok.Literal = l.readNumber()
			if strings.Contains(tok.Literal, ".") {
				tok.Type = FLOAT
			} else {
				tok.Type = INT
			}
			return tok
		}
		tok = l.newToken(ILLEGAL, string(l.ch))
	}

	l.readChar()
	return tok
}

// newToken creates a new token and advances the lexer
func (l *Lexer) newToken(typ TokenType, literal string) Token {
	return Token{Type: typ, Literal: literal, Pos: l.pos}
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// readIdentifier reads an identifier
func (l *Lexer) readIdentifier() string {
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[start:l.pos]
}

// readNumber reads a number (integer or float)
func (l *Lexer) readNumber() string {
	start := l.pos

	// Handle leading dot for floats like .5
	if l.ch == '.' {
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
		return l.input[start:l.pos]
	}

	// Read integer part
	for isDigit(l.ch) {
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' {
		l.readChar()
		// Read fractional part
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	return l.input[start:l.pos]
}

// readString reads a string literal (single-quoted)
func (l *Lexer) readString() string {
	var result strings.Builder

	l.readChar() // skip opening quote

	for {
		if l.ch == 0 {
			break
		}
		if l.ch == '\'' {
			// Check for escaped quote ('')
			if l.peekChar() == '\'' {
				result.WriteByte('\'')
				l.readChar()
				l.readChar()
				continue
			}
			break
		}
		result.WriteByte(l.ch)
		l.readChar()
	}

	l.readChar() // skip closing quote
	return result.String()
}

// isLetter returns true if ch is a letter
func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

// isDigit returns true if ch is a digit
func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}
