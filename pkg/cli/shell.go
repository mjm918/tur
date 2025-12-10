// pkg/cli/shell.go
package cli

import (
	"bufio"
	"io"
	"strings"
)

// Shell represents an interactive SQL shell for TurDB.
// It provides readline-like functionality including multi-line
// statement parsing and command history.
type Shell struct {
	// reader reads input lines
	reader *bufio.Reader

	// output writes normal output
	output io.Writer

	// errOutput writes error messages
	errOutput io.Writer

	// prompt is the primary prompt shown for new statements
	prompt string

	// continuePrompt is shown for multi-line statement continuation
	continuePrompt string

	// history stores command history for recall
	history []string

	// historyIndex tracks current position when navigating history
	historyIndex int

	// maxHistory is the maximum number of history entries to keep
	maxHistory int
}

// NewShell creates a new interactive shell with the given input/output streams.
// If errOutput is nil, errors are written to output.
func NewShell(input io.Reader, output, errOutput io.Writer) *Shell {
	var reader *bufio.Reader
	if input != nil {
		reader = bufio.NewReader(input)
	}

	if errOutput == nil {
		errOutput = output
	}

	return &Shell{
		reader:         reader,
		output:         output,
		errOutput:      errOutput,
		prompt:         "turdb> ",
		continuePrompt: "   ...> ",
		history:        make([]string, 0),
		historyIndex:   0,
		maxHistory:     1000,
	}
}

// SetPrompt changes the primary prompt string.
func (s *Shell) SetPrompt(prompt string) {
	s.prompt = prompt
}

// SetContinuePrompt changes the continuation prompt string.
func (s *Shell) SetContinuePrompt(prompt string) {
	s.continuePrompt = prompt
}

// ReadLine reads a single line from input, stripping trailing whitespace.
// It returns the line and whether EOF was reached.
func (s *Shell) ReadLine() (string, bool) {
	if s.reader == nil {
		return "", true
	}

	line, err := s.reader.ReadString('\n')
	if err != nil {
		// EOF or error
		line = strings.TrimRight(line, " \t\r\n")
		return line, true
	}

	// Strip trailing whitespace including newline
	line = strings.TrimRight(line, " \t\r\n")
	return line, false
}

// ReadStatement reads a complete SQL statement, which may span multiple lines.
// A statement is considered complete when it ends with a semicolon (outside of
// string literals). Returns the statement and whether EOF was reached.
func (s *Shell) ReadStatement() (string, bool) {
	var lines []string
	isFirst := true

	for {
		// Show appropriate prompt
		if s.output != nil {
			if isFirst {
				io.WriteString(s.output, s.prompt)
			} else {
				io.WriteString(s.output, s.continuePrompt)
			}
		}
		isFirst = false

		line, eof := s.ReadLine()

		// Handle empty input
		if eof && line == "" && len(lines) == 0 {
			return "", true
		}

		lines = append(lines, line)
		combined := strings.Join(lines, "\n")

		// Check if statement is complete
		if s.IsComplete(combined) {
			// Add to history if non-empty
			trimmed := strings.TrimSpace(combined)
			if trimmed != "" {
				s.AddHistory(trimmed)
			}
			return combined, false
		}

		// If we hit EOF with an incomplete statement, return what we have
		if eof {
			return combined, true
		}
	}
}

// IsComplete determines if a SQL statement is complete.
// A statement is complete if it ends with a semicolon that is
// not inside a string literal or comment.
func (s *Shell) IsComplete(sql string) bool {
	if sql == "" {
		return false
	}

	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	lastSemicolon := -1

	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		// Handle newlines
		if r == '\n' {
			inLineComment = false
			continue
		}

		// Skip if in line comment
		if inLineComment {
			continue
		}

		// Check for line comment start
		if r == '-' && i+1 < len(runes) && runes[i+1] == '-' {
			inLineComment = true
			i++
			continue
		}

		// Handle string literals
		if r == '\'' && !inDoubleQuote {
			// Check for escaped quote
			if inSingleQuote && i+1 < len(runes) && runes[i+1] == '\'' {
				i++ // Skip escaped quote
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}

		if r == '"' && !inSingleQuote {
			// Check for escaped quote
			if inDoubleQuote && i+1 < len(runes) && runes[i+1] == '"' {
				i++ // Skip escaped quote
				continue
			}
			inDoubleQuote = !inDoubleQuote
			continue
		}

		// Track semicolons outside of strings
		if r == ';' && !inSingleQuote && !inDoubleQuote {
			lastSemicolon = i
			continue
		}
	}

	// Statement is complete if we have a semicolon and strings are closed
	if !inSingleQuote && !inDoubleQuote && lastSemicolon >= 0 {
		return true
	}

	return false
}

// AddHistory adds a statement to the command history.
func (s *Shell) AddHistory(stmt string) {
	// Don't add duplicates of the last entry
	if len(s.history) > 0 && s.history[len(s.history)-1] == stmt {
		return
	}

	s.history = append(s.history, stmt)

	// Trim history if it exceeds max size
	if len(s.history) > s.maxHistory {
		s.history = s.history[len(s.history)-s.maxHistory:]
	}

	// Reset history index to end
	s.historyIndex = len(s.history)
}

// History returns a copy of the command history.
func (s *Shell) History() []string {
	result := make([]string, len(s.history))
	copy(result, s.history)
	return result
}

// ClearHistory removes all entries from the command history.
func (s *Shell) ClearHistory() {
	s.history = make([]string, 0)
	s.historyIndex = 0
}

// HistoryPrev returns the previous history entry, or empty string if at the beginning.
func (s *Shell) HistoryPrev() string {
	if s.historyIndex > 0 {
		s.historyIndex--
		return s.history[s.historyIndex]
	}
	return ""
}

// HistoryNext returns the next history entry, or empty string if at the end.
func (s *Shell) HistoryNext() string {
	if s.historyIndex < len(s.history)-1 {
		s.historyIndex++
		return s.history[s.historyIndex]
	}
	// Reset to end
	s.historyIndex = len(s.history)
	return ""
}
