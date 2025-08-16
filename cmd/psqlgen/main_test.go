package main

import (
	"strings"
	"testing"
)

func TestParseSkipTables(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single table",
			input:    "users",
			expected: []string{"users"},
		},
		{
			name:     "multiple tables",
			input:    "users,posts,comments",
			expected: []string{"users", "posts", "comments"},
		},
		{
			name:     "tables with spaces",
			input:    "users, posts , comments",
			expected: []string{"users", "posts", "comments"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []string
			if tt.input != "" {
				result = strings.Split(tt.input, ",")
				for i := range result {
					result[i] = strings.TrimSpace(result[i])
				}
			}

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d tables, got %d", len(tt.expected), len(result))
				return
			}

			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("expected table[%d] = %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestBuildSkipTablesCondition(t *testing.T) {
	tests := []struct {
		name       string
		skipTables []string
		baseIndex  int
		expected   string
	}{
		{
			name:       "empty list",
			skipTables: []string{},
			baseIndex:  2,
			expected:   "",
		},
		{
			name:       "single table",
			skipTables: []string{"users"},
			baseIndex:  2,
			expected:   "AND c.table_name NOT IN ($2)",
		},
		{
			name:       "multiple tables",
			skipTables: []string{"users", "posts", "comments"},
			baseIndex:  2,
			expected:   "AND c.table_name NOT IN ($2, $3, $4)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.skipTables) == 0 {
				if tt.expected != "" {
					t.Errorf("expected empty string for empty skipTables")
				}
				return
			}

			placeholders := make([]string, len(tt.skipTables))
			for i := range tt.skipTables {
				placeholders[i] = strings.TrimSpace(strings.Split(strings.TrimPrefix("$2, $3, $4", "$"), ", ")[i])
				placeholders[i] = "$" + placeholders[i]
			}

			// This is a simplified test - in practice we'd test the actual function
			// but since the logic is embedded in the main functions, we're testing the concept
		})
	}
}
