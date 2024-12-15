package sqlgen

import (
	"fmt"
	"net/url"
	"strings"
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
