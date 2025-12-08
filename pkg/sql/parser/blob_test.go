package parser

import (
	"bytes"
	"encoding/hex"
	"testing"
	"tur/pkg/types"
)

func TestParser_ParseBlobLiteral(t *testing.T) {
	input := "INSERT INTO t VALUES (x'DEADBEEF')"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	insertStmt, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("Expected *InsertStmt, got %T", stmt)
	}

	if len(insertStmt.Values) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(insertStmt.Values))
	}
	if len(insertStmt.Values[0]) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(insertStmt.Values[0]))
	}

	expr := insertStmt.Values[0][0]
	lit, ok := expr.(*Literal)
	if !ok {
		t.Fatalf("Expected *Literal, got %T", expr)
	}

	if lit.Value.Type() != types.TypeBlob {
		t.Errorf("Expected TypeBlob, got %v", lit.Value.Type())
	}

	expected, _ := hex.DecodeString("DEADBEEF")
	if !bytes.Equal(lit.Value.Blob(), expected) {
		t.Errorf("Expected %X, got %X", expected, lit.Value.Blob())
	}
}

func TestParser_ParseBlobLiteral_LowerCase(t *testing.T) {
	input := "INSERT INTO t VALUES (X'deadbeef')"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	lit := insertStmt.Values[0][0].(*Literal)

	expected, _ := hex.DecodeString("deadbeef") // DecodeString handles mixed case fine, but let's be strict if needed. Actually DecodeString handles it.
	if !bytes.Equal(lit.Value.Blob(), expected) {
		t.Errorf("Expected %X, got %X", expected, lit.Value.Blob())
	}
}
