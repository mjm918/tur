package executor

import (
	"testing"
)

// Tests for stored procedure execution

func TestExecutor_CreateProcedure_Simple(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a simple procedure with no parameters
	_, err := exec.Execute(`
		CREATE PROCEDURE greet()
		BEGIN
			SELECT 'Hello, World!';
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Verify procedure exists in catalog
	proc := exec.catalog.GetProcedure("greet")
	if proc == nil {
		t.Fatal("Procedure 'greet' not found in catalog")
	}
	if proc.Name != "greet" {
		t.Errorf("Expected procedure name 'greet', got '%s'", proc.Name)
	}
	if len(proc.Parameters) != 0 {
		t.Errorf("Expected 0 parameters, got %d", len(proc.Parameters))
	}
}

func TestExecutor_CreateProcedure_WithParams(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure with IN, OUT, and INOUT parameters
	_, err := exec.Execute(`
		CREATE PROCEDURE calculate(IN x INT, OUT result INT, INOUT multiplier INT)
		BEGIN
			SET result = x * multiplier;
			SET multiplier = multiplier + 1;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	proc := exec.catalog.GetProcedure("calculate")
	if proc == nil {
		t.Fatal("Procedure 'calculate' not found in catalog")
	}
	if len(proc.Parameters) != 3 {
		t.Errorf("Expected 3 parameters, got %d", len(proc.Parameters))
	}
}

func TestExecutor_DropProcedure(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure
	_, err := exec.Execute(`
		CREATE PROCEDURE test_proc()
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Verify it exists
	if exec.catalog.GetProcedure("test_proc") == nil {
		t.Fatal("Procedure should exist before drop")
	}

	// Drop the procedure
	_, err = exec.Execute("DROP PROCEDURE test_proc")
	if err != nil {
		t.Fatalf("Drop procedure: %v", err)
	}

	// Verify it's gone
	if exec.catalog.GetProcedure("test_proc") != nil {
		t.Fatal("Procedure should not exist after drop")
	}
}

func TestExecutor_DropProcedure_IfExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Drop non-existent procedure with IF EXISTS should not error
	_, err := exec.Execute("DROP PROCEDURE IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("Drop procedure IF EXISTS should not error: %v", err)
	}
}

func TestExecutor_DropProcedure_NotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Drop non-existent procedure without IF EXISTS should error
	_, err := exec.Execute("DROP PROCEDURE nonexistent")
	if err == nil {
		t.Fatal("Expected error when dropping non-existent procedure")
	}
}

func TestExecutor_CallProcedure_NoParams(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute("CREATE TABLE counters (name TEXT, value INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}
	_, err = exec.Execute("INSERT INTO counters VALUES ('total', 0)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Create a procedure that updates the table
	_, err = exec.Execute(`
		CREATE PROCEDURE increment_counter()
		BEGIN
			UPDATE counters SET value = value + 1 WHERE name = 'total';
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Call the procedure
	_, err = exec.Execute("CALL increment_counter()")
	if err != nil {
		t.Fatalf("Call procedure: %v", err)
	}

	// Verify the update happened
	result, err := exec.Execute("SELECT value FROM counters WHERE name = 'total'")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int() != 1 {
		t.Errorf("Expected value 1, got %d", result.Rows[0][0].Int())
	}
}

func TestExecutor_CallProcedure_WithINParam(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure with IN parameter that copies to OUT
	_, err := exec.Execute(`
		CREATE PROCEDURE double_value(IN x INT, OUT result INT)
		BEGIN
			SET result = x * 2;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Set up session variable to receive OUT value
	_, err = exec.Execute("SET @output = 0")
	if err != nil {
		t.Fatalf("Set @output: %v", err)
	}

	// Call with a literal value for IN parameter
	_, err = exec.Execute("CALL double_value(7, @output)")
	if err != nil {
		t.Fatalf("Call procedure: %v", err)
	}

	// Verify the OUT parameter was set correctly
	if exec.sessionVars["output"].Int() != 14 {
		t.Errorf("Expected @output = 14, got %d", exec.sessionVars["output"].Int())
	}
}

func TestExecutor_SessionVariable_Set(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Set a session variable
	_, err := exec.Execute("SET @myvar = 42")
	if err != nil {
		t.Fatalf("Set session variable: %v", err)
	}

	// Verify it's stored
	if val, ok := exec.sessionVars["myvar"]; !ok {
		t.Fatal("Session variable 'myvar' not found")
	} else if val.Int() != 42 {
		t.Errorf("Expected @myvar = 42, got %d", val.Int())
	}
}

func TestExecutor_SessionVariable_SetExpression(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Set session variables with expressions
	_, err := exec.Execute("SET @x = 10")
	if err != nil {
		t.Fatalf("Set @x: %v", err)
	}
	_, err = exec.Execute("SET @y = @x + 5")
	if err != nil {
		t.Fatalf("Set @y: %v", err)
	}

	if exec.sessionVars["y"].Int() != 15 {
		t.Errorf("Expected @y = 15, got %d", exec.sessionVars["y"].Int())
	}
}

func TestExecutor_Procedure_LocalVariables(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure that uses local variables
	_, err := exec.Execute(`
		CREATE PROCEDURE calc_sum(IN a INT, IN b INT, OUT total INT)
		BEGIN
			DECLARE temp INT;
			SET temp = a + b;
			SET total = temp;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Set up session variable to receive OUT value
	_, err = exec.Execute("SET @result = 0")
	if err != nil {
		t.Fatalf("Set @result: %v", err)
	}

	// Call the procedure
	_, err = exec.Execute("CALL calc_sum(10, 20, @result)")
	if err != nil {
		t.Fatalf("Call procedure: %v", err)
	}

	// Check the OUT parameter was set
	if exec.sessionVars["result"].Int() != 30 {
		t.Errorf("Expected @result = 30, got %d", exec.sessionVars["result"].Int())
	}
}

func TestExecutor_Procedure_Loop(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure with a loop that calculates a sum
	_, err := exec.Execute(`
		CREATE PROCEDURE sum_to_n(IN n INT, OUT total INT)
		BEGIN
			DECLARE counter INT;
			SET counter = 0;
			SET total = 0;
			myloop: LOOP
				IF counter >= n THEN
					LEAVE myloop;
				END IF;
				SET counter = counter + 1;
				SET total = total + counter;
			END LOOP;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Set up session variable to receive OUT value
	_, err = exec.Execute("SET @sum = 0")
	if err != nil {
		t.Fatalf("Set @sum: %v", err)
	}

	// Call the procedure to sum 1+2+3+4+5 = 15
	_, err = exec.Execute("CALL sum_to_n(5, @sum)")
	if err != nil {
		t.Fatalf("Call procedure: %v", err)
	}

	// Verify the sum is correct
	if exec.sessionVars["sum"].Int() != 15 {
		t.Errorf("Expected @sum = 15 (1+2+3+4+5), got %d", exec.sessionVars["sum"].Int())
	}
}

func TestExecutor_Procedure_INOUT_Param(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure that modifies an INOUT parameter
	_, err := exec.Execute(`
		CREATE PROCEDURE double_it(INOUT val INT)
		BEGIN
			SET val = val * 2;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Initialize session variable
	_, err = exec.Execute("SET @num = 5")
	if err != nil {
		t.Fatalf("Set @num: %v", err)
	}

	// Call the procedure
	_, err = exec.Execute("CALL double_it(@num)")
	if err != nil {
		t.Fatalf("Call procedure: %v", err)
	}

	// Check the INOUT parameter was modified
	if exec.sessionVars["num"].Int() != 10 {
		t.Errorf("Expected @num = 10, got %d", exec.sessionVars["num"].Int())
	}
}

func TestExecutor_Procedure_MultipleOUT(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure with multiple OUT parameters
	_, err := exec.Execute(`
		CREATE PROCEDURE split_sum(IN total INT, OUT half1 INT, OUT half2 INT)
		BEGIN
			SET half1 = total / 2;
			SET half2 = total - half1;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Initialize session variables
	_, err = exec.Execute("SET @h1 = 0")
	if err != nil {
		t.Fatalf("Set @h1: %v", err)
	}
	_, err = exec.Execute("SET @h2 = 0")
	if err != nil {
		t.Fatalf("Set @h2: %v", err)
	}

	// Call the procedure
	_, err = exec.Execute("CALL split_sum(100, @h1, @h2)")
	if err != nil {
		t.Fatalf("Call procedure: %v", err)
	}

	// Check the OUT parameters
	if exec.sessionVars["h1"].Int() != 50 {
		t.Errorf("Expected @h1 = 50, got %d", exec.sessionVars["h1"].Int())
	}
	if exec.sessionVars["h2"].Int() != 50 {
		t.Errorf("Expected @h2 = 50, got %d", exec.sessionVars["h2"].Int())
	}
}

func TestExecutor_CallProcedure_NotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Call a non-existent procedure
	_, err := exec.Execute("CALL nonexistent()")
	if err == nil {
		t.Fatal("Expected error when calling non-existent procedure")
	}
}

func TestExecutor_CallProcedure_WrongArgCount(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure with 2 parameters
	_, err := exec.Execute(`
		CREATE PROCEDURE two_params(IN a INT, IN b INT)
		BEGIN
			SELECT a + b;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Call with wrong number of arguments
	_, err = exec.Execute("CALL two_params(1)")
	if err == nil {
		t.Fatal("Expected error when calling with wrong argument count")
	}
}

func TestExecutor_CreateProcedure_AlreadyExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a procedure
	_, err := exec.Execute(`
		CREATE PROCEDURE existing()
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Fatalf("Create procedure: %v", err)
	}

	// Try to create it again
	_, err = exec.Execute(`
		CREATE PROCEDURE existing()
		BEGIN
			SELECT 2;
		END
	`)
	if err == nil {
		t.Fatal("Expected error when creating duplicate procedure")
	}
}
