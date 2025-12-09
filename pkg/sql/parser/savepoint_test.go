package parser

import (
	"testing"
)

func TestParser_Savepoint(t *testing.T) {
	input := "SAVEPOINT sp1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	savepoint, ok := stmt.(*SavepointStmt)
	if !ok {
		t.Fatalf("Expected *SavepointStmt, got %T", stmt)
	}

	if savepoint.Name != "sp1" {
		t.Errorf("Name = %q, want 'sp1'", savepoint.Name)
	}
}

func TestParser_Savepoint_CaseInsensitive(t *testing.T) {
	tests := []struct {
		input string
		name  string
	}{
		{"SAVEPOINT my_savepoint", "my_savepoint"},
		{"savepoint MyPoint", "MyPoint"},
		{"Savepoint test_sp", "test_sp"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			p := New(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			savepoint, ok := stmt.(*SavepointStmt)
			if !ok {
				t.Fatalf("Expected *SavepointStmt, got %T", stmt)
			}

			if savepoint.Name != tt.name {
				t.Errorf("Name = %q, want %q", savepoint.Name, tt.name)
			}
		})
	}
}

func TestParser_RollbackTo(t *testing.T) {
	input := "ROLLBACK TO sp1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rollbackTo, ok := stmt.(*RollbackToStmt)
	if !ok {
		t.Fatalf("Expected *RollbackToStmt, got %T", stmt)
	}

	if rollbackTo.Name != "sp1" {
		t.Errorf("Name = %q, want 'sp1'", rollbackTo.Name)
	}
}

func TestParser_RollbackToSavepoint(t *testing.T) {
	// ROLLBACK TO SAVEPOINT is also valid SQLite syntax
	input := "ROLLBACK TO SAVEPOINT my_sp"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rollbackTo, ok := stmt.(*RollbackToStmt)
	if !ok {
		t.Fatalf("Expected *RollbackToStmt, got %T", stmt)
	}

	if rollbackTo.Name != "my_sp" {
		t.Errorf("Name = %q, want 'my_sp'", rollbackTo.Name)
	}
}

func TestParser_Release(t *testing.T) {
	input := "RELEASE sp1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	release, ok := stmt.(*ReleaseStmt)
	if !ok {
		t.Fatalf("Expected *ReleaseStmt, got %T", stmt)
	}

	if release.Name != "sp1" {
		t.Errorf("Name = %q, want 'sp1'", release.Name)
	}
}

func TestParser_ReleaseSavepoint(t *testing.T) {
	// RELEASE SAVEPOINT is also valid SQLite syntax
	input := "RELEASE SAVEPOINT checkpoint1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	release, ok := stmt.(*ReleaseStmt)
	if !ok {
		t.Fatalf("Expected *ReleaseStmt, got %T", stmt)
	}

	if release.Name != "checkpoint1" {
		t.Errorf("Name = %q, want 'checkpoint1'", release.Name)
	}
}
