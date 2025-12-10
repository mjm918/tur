// pkg/vdbe/json_functions.go
// JSON function implementations for TurDB.
package vdbe

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"tur/pkg/types"
)

// RegisterJSONFunctions registers all JSON functions with the registry
func RegisterJSONFunctions(r *FunctionRegistry) {
	// JSON_EXTRACT - Extract value from JSON using path
	r.Register(&ScalarFunction{
		Name:     "JSON_EXTRACT",
		NumArgs:  2,
		Function: builtinJSONExtract,
	})

	// JSON_UNQUOTE - Remove quotes from JSON string
	r.Register(&ScalarFunction{
		Name:     "JSON_UNQUOTE",
		NumArgs:  1,
		Function: builtinJSONUnquote,
	})

	// JSON_ARRAY - Create JSON array from arguments
	r.Register(&ScalarFunction{
		Name:     "JSON_ARRAY",
		NumArgs:  -1, // Variadic
		Function: builtinJSONArray,
	})

	// JSON_OBJECT - Create JSON object from key-value pairs
	r.Register(&ScalarFunction{
		Name:     "JSON_OBJECT",
		NumArgs:  -1, // Variadic (must be even number)
		Function: builtinJSONObject,
	})

	// JSON_TYPE - Return type of JSON value
	r.Register(&ScalarFunction{
		Name:     "JSON_TYPE",
		NumArgs:  1,
		Function: builtinJSONType,
	})

	// JSON_VALID - Check if string is valid JSON
	r.Register(&ScalarFunction{
		Name:     "JSON_VALID",
		NumArgs:  1,
		Function: builtinJSONValid,
	})

	// JSON_LENGTH - Return length of JSON array or object
	r.Register(&ScalarFunction{
		Name:     "JSON_LENGTH",
		NumArgs:  -1, // 1 or 2 args (json, [path])
		Function: builtinJSONLength,
	})

	// JSON_KEYS - Return keys of JSON object
	r.Register(&ScalarFunction{
		Name:     "JSON_KEYS",
		NumArgs:  -1, // 1 or 2 args (json, [path])
		Function: builtinJSONKeys,
	})

	// JSON_CONTAINS - Check if JSON contains value
	r.Register(&ScalarFunction{
		Name:     "JSON_CONTAINS",
		NumArgs:  -1, // 2 or 3 args (target, candidate, [path])
		Function: builtinJSONContains,
	})

	// JSON_SET - Set value in JSON document
	r.Register(&ScalarFunction{
		Name:     "JSON_SET",
		NumArgs:  -1, // 3+ args (json, path, val, [path, val]...)
		Function: builtinJSONSet,
	})

	// JSON_REMOVE - Remove value from JSON document
	r.Register(&ScalarFunction{
		Name:     "JSON_REMOVE",
		NumArgs:  -1, // 2+ args (json, path, [path]...)
		Function: builtinJSONRemove,
	})
}

// getJSONValue converts a types.Value to its JSON string representation
func getJSONValue(v types.Value) (string, error) {
	switch v.Type() {
	case types.TypeJSON:
		return v.JSON(), nil
	case types.TypeText:
		return v.Text(), nil
	case types.TypeNull:
		return "null", nil
	default:
		return "", fmt.Errorf("expected JSON or TEXT value, got %v", v.Type())
	}
}

// valueToJSON converts a types.Value to a JSON-encodable value
func valueToJSON(v types.Value) interface{} {
	switch v.Type() {
	case types.TypeNull:
		return nil
	case types.TypeInt:
		return v.Int()
	case types.TypeFloat:
		return v.Float()
	case types.TypeText:
		return v.Text()
	case types.TypeJSON:
		var result interface{}
		json.Unmarshal([]byte(v.JSON()), &result)
		return result
	default:
		return v.Text()
	}
}

// jsonPathExtract extracts a value from JSON using a path like $.key.subkey[0]
func jsonPathExtract(jsonStr, path string) (string, error) {
	if !strings.HasPrefix(path, "$") {
		return "", fmt.Errorf("JSON path must start with $")
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "", fmt.Errorf("invalid JSON: %v", err)
	}

	// Parse and follow the path
	current := data
	pathParts := parseJSONPath(path[1:]) // Skip the leading $

	for _, part := range pathParts {
		if part == "" {
			continue
		}

		// Check if this is an array index
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			indexStr := part[1 : len(part)-1]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return "", fmt.Errorf("invalid array index: %s", indexStr)
			}

			arr, ok := current.([]interface{})
			if !ok {
				return "", fmt.Errorf("expected array at path")
			}

			if index < 0 || index >= len(arr) {
				return "null", nil
			}
			current = arr[index]
		} else {
			// Object key access
			obj, ok := current.(map[string]interface{})
			if !ok {
				return "", fmt.Errorf("expected object at path")
			}

			val, exists := obj[part]
			if !exists {
				return "null", nil
			}
			current = val
		}
	}

	// Convert result back to JSON string
	result, err := json.Marshal(current)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// parseJSONPath parses a JSON path into components
// e.g., ".name.items[0]" -> ["name", "items", "[0]"]
func parseJSONPath(path string) []string {
	var parts []string
	var current strings.Builder

	for i := 0; i < len(path); i++ {
		ch := path[i]
		switch ch {
		case '.':
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		case '[':
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			// Read until ]
			start := i
			for i < len(path) && path[i] != ']' {
				i++
			}
			if i < len(path) {
				parts = append(parts, path[start:i+1])
			}
		default:
			current.WriteByte(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// builtinJSONExtract extracts a value from JSON using a path
func builtinJSONExtract(args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewNull()
	}

	jsonStr, err := getJSONValue(args[0])
	if err != nil {
		return types.NewNull()
	}

	path := args[1].Text()
	result, err := jsonPathExtract(jsonStr, path)
	if err != nil {
		return types.NewNull()
	}

	return types.NewText(result)
}

// builtinJSONUnquote removes quotes from a JSON string value
func builtinJSONUnquote(args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewNull()
	}

	var str string
	if args[0].Type() == types.TypeJSON {
		str = args[0].JSON()
	} else {
		str = args[0].Text()
	}

	// If it's a quoted string, unquote it
	if len(str) >= 2 && str[0] == '"' && str[len(str)-1] == '"' {
		var unquoted string
		err := json.Unmarshal([]byte(str), &unquoted)
		if err != nil {
			return types.NewText(str)
		}
		return types.NewText(unquoted)
	}

	return types.NewText(str)
}

// builtinJSONArray creates a JSON array from arguments
func builtinJSONArray(args []types.Value) types.Value {
	arr := make([]interface{}, len(args))
	for i, arg := range args {
		arr[i] = valueToJSON(arg)
	}

	result, err := json.Marshal(arr)
	if err != nil {
		return types.NewNull()
	}

	return types.NewJSON(string(result))
}

// builtinJSONObject creates a JSON object from key-value pairs
func builtinJSONObject(args []types.Value) types.Value {
	if len(args)%2 != 0 {
		return types.NewNull() // Must have even number of arguments
	}

	obj := make(map[string]interface{})
	for i := 0; i < len(args); i += 2 {
		key := args[i].Text()
		val := valueToJSON(args[i+1])
		obj[key] = val
	}

	result, err := json.Marshal(obj)
	if err != nil {
		return types.NewNull()
	}

	return types.NewJSON(string(result))
}

// builtinJSONType returns the type of a JSON value
func builtinJSONType(args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewNull()
	}

	jsonStr, err := getJSONValue(args[0])
	if err != nil {
		return types.NewNull()
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return types.NewNull()
	}

	switch v := data.(type) {
	case nil:
		return types.NewText("NULL")
	case bool:
		return types.NewText("BOOLEAN")
	case float64:
		// Check if it's an integer
		if v == float64(int64(v)) {
			return types.NewText("INTEGER")
		}
		return types.NewText("DOUBLE")
	case string:
		return types.NewText("STRING")
	case []interface{}:
		return types.NewText("ARRAY")
	case map[string]interface{}:
		return types.NewText("OBJECT")
	default:
		return types.NewNull()
	}
}

// builtinJSONValid checks if a string is valid JSON
func builtinJSONValid(args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewInt(0)
	}

	var jsonStr string
	if args[0].Type() == types.TypeJSON {
		jsonStr = args[0].JSON()
	} else {
		jsonStr = args[0].Text()
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return types.NewInt(0)
	}

	return types.NewInt(1)
}

// builtinJSONLength returns the length of a JSON array or object
func builtinJSONLength(args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewNull()
	}

	jsonStr, err := getJSONValue(args[0])
	if err != nil {
		return types.NewNull()
	}

	// If path provided, extract first
	if len(args) >= 2 {
		path := args[1].Text()
		jsonStr, err = jsonPathExtract(jsonStr, path)
		if err != nil {
			return types.NewNull()
		}
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return types.NewNull()
	}

	switch v := data.(type) {
	case []interface{}:
		return types.NewInt(int64(len(v)))
	case map[string]interface{}:
		return types.NewInt(int64(len(v)))
	default:
		// Scalar values have length 1
		return types.NewInt(1)
	}
}

// builtinJSONKeys returns the keys of a JSON object as a JSON array
func builtinJSONKeys(args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewNull()
	}

	jsonStr, err := getJSONValue(args[0])
	if err != nil {
		return types.NewNull()
	}

	// If path provided, extract first
	if len(args) >= 2 {
		path := args[1].Text()
		jsonStr, err = jsonPathExtract(jsonStr, path)
		if err != nil {
			return types.NewNull()
		}
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return types.NewNull()
	}

	obj, ok := data.(map[string]interface{})
	if !ok {
		return types.NewNull()
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result, err := json.Marshal(keys)
	if err != nil {
		return types.NewNull()
	}

	return types.NewJSON(string(result))
}

// builtinJSONContains checks if JSON target contains the candidate
func builtinJSONContains(args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewInt(0)
	}

	targetStr, err := getJSONValue(args[0])
	if err != nil {
		return types.NewInt(0)
	}

	candidateStr := args[1].Text()

	// If path provided, extract from target first
	if len(args) >= 3 {
		path := args[2].Text()
		targetStr, err = jsonPathExtract(targetStr, path)
		if err != nil {
			return types.NewInt(0)
		}
	}

	var target, candidate interface{}
	if err := json.Unmarshal([]byte(targetStr), &target); err != nil {
		return types.NewInt(0)
	}
	if err := json.Unmarshal([]byte(candidateStr), &candidate); err != nil {
		return types.NewInt(0)
	}

	if jsonContains(target, candidate) {
		return types.NewInt(1)
	}
	return types.NewInt(0)
}

// jsonContains checks if target contains candidate
func jsonContains(target, candidate interface{}) bool {
	switch t := target.(type) {
	case []interface{}:
		// Array contains candidate if any element equals candidate
		for _, elem := range t {
			if jsonEquals(elem, candidate) {
				return true
			}
		}
		return false
	case map[string]interface{}:
		// Object contains candidate if candidate is object and all its keys exist with matching values
		candObj, ok := candidate.(map[string]interface{})
		if !ok {
			return false
		}
		for k, v := range candObj {
			if tv, exists := t[k]; !exists || !jsonEquals(tv, v) {
				return false
			}
		}
		return true
	default:
		return jsonEquals(target, candidate)
	}
}

// jsonEquals checks if two JSON values are equal
func jsonEquals(a, b interface{}) bool {
	aBytes, _ := json.Marshal(a)
	bBytes, _ := json.Marshal(b)
	return string(aBytes) == string(bBytes)
}

// builtinJSONSet sets values in a JSON document
func builtinJSONSet(args []types.Value) types.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return types.NewNull()
	}

	jsonStr, err := getJSONValue(args[0])
	if err != nil {
		return types.NewNull()
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return types.NewNull()
	}

	// Process path-value pairs
	for i := 1; i < len(args); i += 2 {
		path := args[i].Text()
		value := valueToJSON(args[i+1])
		data = jsonSetPath(data, path, value)
	}

	result, err := json.Marshal(data)
	if err != nil {
		return types.NewNull()
	}

	return types.NewJSON(string(result))
}

// jsonSetPath sets a value at the given path in the JSON structure
func jsonSetPath(data interface{}, path string, value interface{}) interface{} {
	if !strings.HasPrefix(path, "$") {
		return data
	}

	parts := parseJSONPath(path[1:])
	if len(parts) == 0 {
		return value
	}

	return setPathRecursive(data, parts, value)
}

// setPathRecursive recursively sets a value in the JSON structure
func setPathRecursive(data interface{}, parts []string, value interface{}) interface{} {
	if len(parts) == 0 {
		return value
	}

	part := parts[0]
	remaining := parts[1:]

	if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
		// Array index
		indexStr := part[1 : len(part)-1]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return data
		}

		arr, ok := data.([]interface{})
		if !ok {
			arr = make([]interface{}, 0)
		}

		// Extend array if needed
		for len(arr) <= index {
			arr = append(arr, nil)
		}

		if len(remaining) == 0 {
			arr[index] = value
		} else {
			arr[index] = setPathRecursive(arr[index], remaining, value)
		}
		return arr
	}

	// Object key
	obj, ok := data.(map[string]interface{})
	if !ok {
		obj = make(map[string]interface{})
	}

	if len(remaining) == 0 {
		obj[part] = value
	} else {
		obj[part] = setPathRecursive(obj[part], remaining, value)
	}
	return obj
}

// builtinJSONRemove removes values from a JSON document
func builtinJSONRemove(args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewNull()
	}

	jsonStr, err := getJSONValue(args[0])
	if err != nil {
		return types.NewNull()
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return types.NewNull()
	}

	// Process each path to remove
	for i := 1; i < len(args); i++ {
		path := args[i].Text()
		data = jsonRemovePath(data, path)
	}

	result, err := json.Marshal(data)
	if err != nil {
		return types.NewNull()
	}

	return types.NewJSON(string(result))
}

// jsonRemovePath removes a value at the given path in the JSON structure
func jsonRemovePath(data interface{}, path string) interface{} {
	if !strings.HasPrefix(path, "$") {
		return data
	}

	parts := parseJSONPath(path[1:])
	if len(parts) == 0 {
		return nil
	}

	return removePathRecursive(data, parts)
}

// removePathRecursive recursively removes a value from the JSON structure
func removePathRecursive(data interface{}, parts []string) interface{} {
	if len(parts) == 0 {
		return data
	}

	part := parts[0]
	remaining := parts[1:]

	if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
		// Array index
		indexStr := part[1 : len(part)-1]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return data
		}

		arr, ok := data.([]interface{})
		if !ok || index < 0 || index >= len(arr) {
			return data
		}

		if len(remaining) == 0 {
			// Remove this element
			return append(arr[:index], arr[index+1:]...)
		}

		arr[index] = removePathRecursive(arr[index], remaining)
		return arr
	}

	// Object key
	obj, ok := data.(map[string]interface{})
	if !ok {
		return data
	}

	if len(remaining) == 0 {
		// Remove this key
		delete(obj, part)
		return obj
	}

	if val, exists := obj[part]; exists {
		obj[part] = removePathRecursive(val, remaining)
	}
	return obj
}
