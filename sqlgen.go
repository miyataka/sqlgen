package sqlgen

import (
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// SnakeToPascal converts a snake_case string to PascalCase using golang.org/x/text/cases
func SnakeToPascal(input string) string {
	// Split the string by underscores
	words := strings.Split(input, "_")

	// Create a Title casing transformer
	caser := cases.Title(language.Und) // "Und" is for undetermined language

	// Capitalize the first letter of each word
	for i, word := range words {
		words[i] = caser.String(word)
	}

	// Join the words together to form PascalCase
	return strings.Join(words, "")
}

// GetTableName extracts the table name from an INSERT SQL statement.
func GetTableName(sql string) (string, error) {
	// Normalize and trim the SQL statement
	sql = strings.TrimSpace(sql)
	sql = strings.ToUpper(sql)

	// Regular expression to match the table name after "INSERT INTO"
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+([^\s\(\)]+)`)
	match := re.FindStringSubmatch(sql)
	if len(match) == 2 {
		return strings.ToLower(match[1]), nil
	}

	re2 := regexp.MustCompile(`FROM\s+([^\s\(\)]+)`)
	match2 := re2.FindStringSubmatch(sql)
	if len(match2) < 2 {
		return "", fmt.Errorf("failed to extract table name from SQL: %s", sql)
	}

	// Return the table name
	return strings.ToLower(match2[1]), nil
}
