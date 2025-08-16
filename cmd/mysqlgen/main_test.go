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
		expected   string
	}{
		{
			name:       "empty list",
			skipTables: []string{},
			expected:   "",
		},
		{
			name:       "single table",
			skipTables: []string{"users"},
			expected:   "AND c.TABLE_NAME NOT IN (?)",
		},
		{
			name:       "multiple tables",
			skipTables: []string{"users", "posts", "comments"},
			expected:   "AND c.TABLE_NAME NOT IN (?, ?, ?)",
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
				placeholders[i] = "?"
			}
			result := "AND c.TABLE_NAME NOT IN (" + strings.Join(placeholders, ", ") + ")"
			
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}