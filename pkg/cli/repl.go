// pkg/cli/repl.go
package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"tur/pkg/schema"
	"tur/pkg/turdb"
	"tur/pkg/types"
)

// REPL provides a Read-Eval-Print Loop for interactive SQL execution.
type REPL struct {
	// db is the database connection
	db *turdb.DB

	// shell handles input/output and statement parsing
	shell *Shell

	// output is where results are written
	output io.Writer

	// errOutput is where errors are written
	errOutput io.Writer

	// running indicates if the REPL is currently running
	running bool

	// exitRequested indicates that .exit was called
	exitRequested bool
}

// NewREPL creates a new REPL with the given database path.
// Output is written to stdout and errors to stderr.
func NewREPL(dbPath string, output, errOutput io.Writer) (*REPL, error) {
	return NewREPLWithInput(dbPath, os.Stdin, output, errOutput)
}

// NewREPLWithInput creates a new REPL with custom input/output streams.
// This is useful for testing or scripted operation.
func NewREPLWithInput(dbPath string, input io.Reader, output, errOutput io.Writer) (*REPL, error) {
	// Handle :memory: as in-memory database
	var db *turdb.DB
	var err error

	if dbPath == ":memory:" {
		// Create a temporary file for in-memory simulation
		// TODO: Implement true in-memory database support in turdb
		tmpFile, err := os.CreateTemp("", "turdb-memory-*.db")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}
		dbPath = tmpFile.Name()
		tmpFile.Close()
	}

	db, err = turdb.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	shell := NewShell(input, output, errOutput)

	return &REPL{
		db:        db,
		shell:     shell,
		output:    output,
		errOutput: errOutput,
		running:   false,
	}, nil
}

// Close closes the REPL and underlying database connection.
func (r *REPL) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// Run starts the REPL loop, reading and executing statements until
// EOF or .exit command.
func (r *REPL) Run() {
	r.running = true
	r.exitRequested = false

	// Print welcome message
	fmt.Fprintln(r.output, "TurDB version 0.1.0")
	fmt.Fprintln(r.output, "Enter \".help\" for usage hints.")

	for r.running && !r.exitRequested {
		stmt, eof := r.shell.ReadStatement()

		if eof && stmt == "" {
			// Clean EOF, exit gracefully
			fmt.Fprintln(r.output)
			break
		}

		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Check for dot commands
		if strings.HasPrefix(stmt, ".") {
			r.handleDotCommand(stmt)
			continue
		}

		// Execute SQL statement
		if err := r.ExecuteStatement(stmt); err != nil {
			r.printError(err)
		}

		if eof {
			break
		}
	}

	r.running = false
}

// ExecuteStatement executes a single SQL statement and displays the result.
func (r *REPL) ExecuteStatement(sql string) error {
	result, err := r.db.Exec(sql)
	if err != nil {
		return err
	}

	r.displayResult(result)
	return nil
}

// displayResult formats and prints query results.
func (r *REPL) displayResult(result *turdb.QueryResult) {
	if result == nil {
		return
	}

	// If no columns, this was not a SELECT - show rows affected
	if len(result.Columns) == 0 {
		if result.RowsAffected > 0 {
			fmt.Fprintf(r.output, "Rows affected: %d\n", result.RowsAffected)
		}
		return
	}

	// Display as table
	r.displayTable(result.Columns, result.Rows)
}

// displayTable formats results as an ASCII table.
func (r *REPL) displayTable(columns []string, rows [][]interface{}) {
	if len(columns) == 0 {
		return
	}

	// Calculate column widths
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}

	// Check row data for wider values
	for _, row := range rows {
		for i, val := range row {
			if i < len(widths) {
				s := formatValue(val)
				if len(s) > widths[i] {
					widths[i] = len(s)
				}
			}
		}
	}

	// Print header separator
	r.printSeparator(widths)

	// Print header
	r.printRow(columns, widths)

	// Print header separator
	r.printSeparator(widths)

	// Print rows
	for _, row := range rows {
		r.printDataRow(row, widths)
	}

	// Print footer separator
	r.printSeparator(widths)

	// Print row count
	fmt.Fprintf(r.output, "%d row(s)\n", len(rows))
}

// printSeparator prints a horizontal line separator.
func (r *REPL) printSeparator(widths []int) {
	fmt.Fprint(r.output, "+")
	for _, w := range widths {
		fmt.Fprint(r.output, strings.Repeat("-", w+2))
		fmt.Fprint(r.output, "+")
	}
	fmt.Fprintln(r.output)
}

// printRow prints a row of string values.
func (r *REPL) printRow(values []string, widths []int) {
	fmt.Fprint(r.output, "|")
	for i, val := range values {
		w := widths[i]
		fmt.Fprintf(r.output, " %-*s |", w, val)
	}
	fmt.Fprintln(r.output)
}

// printDataRow prints a row of interface{} values.
func (r *REPL) printDataRow(row []interface{}, widths []int) {
	fmt.Fprint(r.output, "|")
	for i, val := range row {
		w := widths[i]
		s := formatValue(val)
		fmt.Fprintf(r.output, " %-*s |", w, s)
	}
	fmt.Fprintln(r.output)
}

// formatValue converts a value to its string representation.
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		return val
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case []byte:
		return fmt.Sprintf("[blob %d bytes]", len(val))
	case []float32:
		if len(val) > 4 {
			return fmt.Sprintf("[vector dim=%d]", len(val))
		}
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// handleDotCommand processes special dot commands.
func (r *REPL) handleDotCommand(cmd string) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return
	}

	switch strings.ToLower(parts[0]) {
	case ".exit", ".quit":
		r.exitRequested = true
	case ".help":
		r.printHelp()
	case ".tables":
		r.showTables()
	case ".schema":
		if len(parts) > 1 {
			r.showSchema(parts[1])
		} else {
			r.showAllSchemas()
		}
	default:
		fmt.Fprintf(r.errOutput, "Unknown command: %s\n", parts[0])
		fmt.Fprintln(r.errOutput, "Use \".help\" for usage hints.")
	}
}

// printHelp displays help information.
func (r *REPL) printHelp() {
	help := `
.exit              Exit this program
.help              Show this help message
.quit              Exit this program
.schema [TABLE]    Show CREATE statement for table(s)
.tables            List all tables

Enter SQL statements terminated with a semicolon.
Multi-line statements are supported.
`
	fmt.Fprintln(r.output, help)
}

// showTables lists all tables in the database.
func (r *REPL) showTables() {
	catalog := r.db.Catalog()
	if catalog == nil {
		return
	}

	tables := catalog.ListTables()
	if len(tables) == 0 {
		fmt.Fprintln(r.output, "(no tables)")
		return
	}

	for _, name := range tables {
		fmt.Fprintln(r.output, name)
	}
}

// showSchema shows the CREATE statement for a specific table.
func (r *REPL) showSchema(tableName string) {
	catalog := r.db.Catalog()
	if catalog == nil {
		return
	}

	table := catalog.GetTable(tableName)
	if table == nil {
		fmt.Fprintf(r.errOutput, "Error: no such table: %s\n", tableName)
		return
	}

	fmt.Fprintln(r.output, generateCreateSQL(table))
}

// showAllSchemas shows CREATE statements for all tables.
func (r *REPL) showAllSchemas() {
	catalog := r.db.Catalog()
	if catalog == nil {
		return
	}

	tables := catalog.ListTables()
	for _, name := range tables {
		table := catalog.GetTable(name)
		if table != nil {
			fmt.Fprintln(r.output, generateCreateSQL(table))
		}
	}
}

// generateCreateSQL generates a CREATE TABLE statement from a TableDef.
func generateCreateSQL(table *schema.TableDef) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(table.Name)
	sb.WriteString(" (")

	for i, col := range table.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(valueTypeString(col.Type))

		if col.PrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}
		if col.NotNull {
			sb.WriteString(" NOT NULL")
		}
	}

	sb.WriteString(");")
	return sb.String()
}

// valueTypeString returns the SQL type name for a ValueType.
func valueTypeString(vt types.ValueType) string {
	switch vt {
	case types.TypeNull:
		return "NULL"
	case types.TypeSmallInt:
		return "SMALLINT"
	case types.TypeInt32:
		return "INT"
	case types.TypeBigInt:
		return "BIGINT"
	case types.TypeSerial:
		return "SERIAL"
	case types.TypeBigSerial:
		return "BIGSERIAL"
	case types.TypeFloat:
		return "REAL"
	case types.TypeText:
		return "TEXT"
	case types.TypeBlob:
		return "BLOB"
	case types.TypeVector:
		return "VECTOR"
	default:
		return "UNKNOWN"
	}
}

// printError prints an error message to the error output.
func (r *REPL) printError(err error) {
	fmt.Fprintf(r.errOutput, "Error: %v\n", err)
}
