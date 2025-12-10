// pkg/cli/shell_test.go
package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewShell(t *testing.T) {
	input := strings.NewReader("")
	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	shell := NewShell(input, output, errOutput)

	if shell == nil {
		t.Fatal("NewShell returned nil")
	}

	if shell.prompt != "turdb> " {
		t.Errorf("expected default prompt 'turdb> ', got %q", shell.prompt)
	}

	if shell.continuePrompt != "   ...> " {
		t.Errorf("expected continue prompt '   ...> ', got %q", shell.continuePrompt)
	}
}

func TestShell_SetPrompt(t *testing.T) {
	shell := NewShell(nil, nil, nil)
	shell.SetPrompt("custom> ")

	if shell.prompt != "custom> " {
		t.Errorf("expected prompt 'custom> ', got %q", shell.prompt)
	}
}

func TestShell_ReadLine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLine string
		wantEOF  bool
	}{
		{
			name:     "simple line",
			input:    "SELECT 1;\n",
			wantLine: "SELECT 1;",
			wantEOF:  false,
		},
		{
			name:     "empty line",
			input:    "\n",
			wantLine: "",
			wantEOF:  false,
		},
		{
			name:     "EOF",
			input:    "",
			wantLine: "",
			wantEOF:  true,
		},
		{
			name:     "line with trailing whitespace",
			input:    "SELECT * FROM t;  \n",
			wantLine: "SELECT * FROM t;",
			wantEOF:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := strings.NewReader(tt.input)
			output := &bytes.Buffer{}
			shell := NewShell(input, output, nil)

			line, eof := shell.ReadLine()

			if line != tt.wantLine {
				t.Errorf("ReadLine() line = %q, want %q", line, tt.wantLine)
			}
			if eof != tt.wantEOF {
				t.Errorf("ReadLine() eof = %v, want %v", eof, tt.wantEOF)
			}
		})
	}
}

func TestShell_ReadStatement_SingleLine(t *testing.T) {
	input := strings.NewReader("SELECT 1;\n")
	output := &bytes.Buffer{}
	shell := NewShell(input, output, nil)

	stmt, eof := shell.ReadStatement()

	if eof {
		t.Error("ReadStatement returned unexpected EOF")
	}

	expected := "SELECT 1;"
	if stmt != expected {
		t.Errorf("ReadStatement() = %q, want %q", stmt, expected)
	}
}

func TestShell_ReadStatement_MultiLine(t *testing.T) {
	// Multi-line statement: semicolon on second line
	input := strings.NewReader("SELECT *\nFROM users;\n")
	output := &bytes.Buffer{}
	shell := NewShell(input, output, nil)

	stmt, eof := shell.ReadStatement()

	if eof {
		t.Error("ReadStatement returned unexpected EOF")
	}

	expected := "SELECT *\nFROM users;"
	if stmt != expected {
		t.Errorf("ReadStatement() = %q, want %q", stmt, expected)
	}
}

func TestShell_ReadStatement_EOF(t *testing.T) {
	input := strings.NewReader("")
	output := &bytes.Buffer{}
	shell := NewShell(input, output, nil)

	_, eof := shell.ReadStatement()

	if !eof {
		t.Error("ReadStatement should return EOF for empty input")
	}
}

func TestShell_ReadStatement_IncompleteOnEOF(t *testing.T) {
	// Input without semicolon, then EOF
	input := strings.NewReader("SELECT 1")
	output := &bytes.Buffer{}
	shell := NewShell(input, output, nil)

	stmt, eof := shell.ReadStatement()

	// Should return what we have when EOF is reached
	if !eof {
		t.Error("ReadStatement should return EOF")
	}

	expected := "SELECT 1"
	if stmt != expected {
		t.Errorf("ReadStatement() = %q, want %q", stmt, expected)
	}
}

func TestShell_IsComplete(t *testing.T) {
	shell := NewShell(nil, nil, nil)

	tests := []struct {
		input    string
		complete bool
	}{
		{"SELECT 1;", true},
		{"SELECT 1", false},
		{"", false},
		{";", true},
		{"SELECT * FROM t WHERE a = 'hello;world';", true}, // semicolon inside string doesn't count
		{"SELECT * FROM t WHERE a = 'hello", false},         // unclosed string
		{"SELECT * FROM t; SELECT 2;", true},
		{"-- comment\nSELECT 1;", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := shell.IsComplete(tt.input)
			if got != tt.complete {
				t.Errorf("IsComplete(%q) = %v, want %v", tt.input, got, tt.complete)
			}
		})
	}
}
