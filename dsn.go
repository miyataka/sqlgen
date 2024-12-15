package sqlgen

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// ParseDSN parses a DSN string and returns its components.
func ParseDSN(dsn string) (map[string]string, error) {
	parsedDSN := make(map[string]string)

	// Parse DSN using url.Parse
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	if u.Scheme != "" {
		parsedDSN["protocol"] = u.Scheme
	}

	// Extract user info if available
	if u.User != nil {
		parsedDSN["user"] = u.User.Username()
		if password, hasPassword := u.User.Password(); hasPassword {
			parsedDSN["password"] = password
		}
	}

	// Extract host and port
	hostParts := strings.Split(u.Host, ":")
	if len(hostParts) > 0 {
		parsedDSN["host"] = hostParts[0]
	}
	if len(hostParts) > 1 {
		parsedDSN["port"] = hostParts[1]
	}

	// Extract path (usually the database name)
	if u.Path != "" {
		parsedDSN["database"] = strings.TrimPrefix(u.Path, "/")
	}

	// Extract query parameters
	for key, values := range u.Query() {
		parsedDSN[key] = values[0] // Taking the first value if there are multiple
	}

	return parsedDSN, nil
}

func ParseMysqlDSN(dsn string) (parsedDSN string, dbname string, err error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse DSN: %w", err)
	}
	return cfg.FormatDSN(), cfg.DBName, nil
}

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
	if len(match) < 2 {
		return "", fmt.Errorf("failed to extract table name from SQL: %s", sql)
	}

	// Return the table name (case-insensitive matching)
	return strings.ToLower(match[1]), nil
}
