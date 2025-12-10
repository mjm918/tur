// cmd/turdb/main.go
//
// TurDB CLI - Interactive SQL shell for TurDB databases.
//
// Usage:
//
//	turdb [database-file]
//
// If no database file is specified, opens an in-memory database.
// Use .help for available commands.
package main

import (
	"fmt"
	"os"

	"tur/pkg/cli"
)

func main() {
	// Determine database path from command line
	dbPath := ":memory:"
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}

	// Create and run the REPL
	repl, err := cli.NewREPL(dbPath, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer repl.Close()

	repl.Run()
}
