package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

func TestMySQLIntegration(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysql.Run(ctx,
		"mysql:8",
		mysql.WithDatabase("testdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("password"),
	)
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}
	defer func() {
		if err := mysqlContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	connectionString, err := mysqlContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		t.Fatalf("failed to connect to database: %s", err)
	}
	defer db.Close()

	// Create test tables
	createTableSQL := `
	CREATE TABLE users (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(100) UNIQUE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE posts (
		id INT AUTO_INCREMENT PRIMARY KEY,
		user_id INT,
		title VARCHAR(200) NOT NULL,
		content TEXT,
		published BOOLEAN DEFAULT FALSE,
		FOREIGN KEY (user_id) REFERENCES users(id)
	);

	CREATE TABLE tags (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(50) UNIQUE NOT NULL
	);
	`

	// MySQL requires executing statements one at a time
	statements := strings.Split(createTableSQL, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			_, err = db.Exec(stmt)
			if err != nil {
				t.Fatalf("failed to create tables: %s", err)
			}
		}
	}

	// Test getInsertsStmts
	t.Run("TestGetInsertStatements", func(t *testing.T) {
		database := "testdb" // MySQL database name from container

		insertStmts, err := getInsertsStmts(ctx, db, database, []string{})
		if err != nil {
			t.Fatalf("failed to get insert statements: %s", err)
		}

		if len(insertStmts) != 3 {
			t.Errorf("expected 3 insert statements, got %d", len(insertStmts))
		}

		// Check that INSERT statements are generated for all tables
		hasUsers := false
		hasPosts := false
		hasTags := false

		for _, stmt := range insertStmts {
			if strings.Contains(strings.ToLower(stmt), "insert into users") {
				hasUsers = true
			}
			if strings.Contains(strings.ToLower(stmt), "insert into posts") {
				hasPosts = true
			}
			if strings.Contains(strings.ToLower(stmt), "insert into tags") {
				hasTags = true
			}
		}

		if !hasUsers {
			t.Error("missing INSERT statement for users table")
		}
		if !hasPosts {
			t.Error("missing INSERT statement for posts table")
		}
		if !hasTags {
			t.Error("missing INSERT statement for tags table")
		}
	})

	// Test getSelectByPkStmts
	t.Run("TestGetSelectStatements", func(t *testing.T) {
		database := "testdb"

		selectStmts, err := getSelectsStmts(ctx, db, database, []string{})
		if err != nil {
			t.Fatalf("failed to get select statements: %s", err)
		}

		if len(selectStmts) != 3 {
			t.Errorf("expected 3 select statements, got %d", len(selectStmts))
		}

		// Check that SELECT statements are generated for all tables
		hasUsers := false
		hasPosts := false
		hasTags := false

		for _, stmt := range selectStmts {
			if strings.Contains(strings.ToLower(stmt), "from users") {
				hasUsers = true
			}
			if strings.Contains(strings.ToLower(stmt), "from posts") {
				hasPosts = true
			}
			if strings.Contains(strings.ToLower(stmt), "from tags") {
				hasTags = true
			}
		}

		if !hasUsers {
			t.Error("missing SELECT statement for users table")
		}
		if !hasPosts {
			t.Error("missing SELECT statement for posts table")
		}
		if !hasTags {
			t.Error("missing SELECT statement for tags table")
		}
	})

	// Test skip tables functionality
	t.Run("TestSkipTables", func(t *testing.T) {
		database := "testdb"

		// Skip the tags table
		skipTables := []string{"tags"}
		insertStmts, err := getInsertsStmts(ctx, db, database, skipTables)
		if err != nil {
			t.Fatalf("failed to get insert statements with skip: %s", err)
		}

		// Should only have 2 statements (users and posts)
		if len(insertStmts) != 2 {
			t.Errorf("expected 2 insert statements when skipping tags, got %d", len(insertStmts))
		}

		// Check that tags table is skipped
		for _, stmt := range insertStmts {
			if strings.Contains(strings.ToLower(stmt), "insert into tags") {
				t.Error("tags table should have been skipped")
			}
		}
	})
}

func TestSqlcCommentGeneration(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		action   string
		expected string
	}{
		{
			name:     "Create user comment",
			sql:      "INSERT INTO users (name, email) VALUES (?, ?)",
			action:   "create",
			expected: "-- name: CreateUser :exec",
		},
		{
			name:     "Read post comment",
			sql:      "SELECT * FROM posts WHERE id = ?",
			action:   "read",
			expected: "-- name: GetPostByPk :one",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comment := genComment4Sqlc(tc.sql, tc.action)
			if comment != tc.expected {
				t.Errorf("expected comment %q, got %q", tc.expected, comment)
			}
		})
	}
}