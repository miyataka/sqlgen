package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresIntegration(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	connectionString, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}

	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		t.Fatalf("failed to connect to database: %s", err)
	}
	defer db.Close()

	// Create test tables
	createTableSQL := `
	CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(100) UNIQUE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE posts (
		id SERIAL PRIMARY KEY,
		user_id INTEGER REFERENCES users(id),
		title VARCHAR(200) NOT NULL,
		content TEXT,
		published BOOLEAN DEFAULT FALSE
	);

	CREATE TABLE tags (
		id SERIAL PRIMARY KEY,
		name VARCHAR(50) UNIQUE NOT NULL
	);
	`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("failed to create tables: %s", err)
	}

	// Test getInsertsStmts
	t.Run("TestGetInsertStatements", func(t *testing.T) {
		// Use "public" schema for PostgreSQL
		schema := "public"

		insertStmts, err := getInsertsStmts(ctx, db, schema, []string{})
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
		// Use "public" schema for PostgreSQL
		schema := "public"

		selectStmts, err := getSelectByPkStmts(ctx, db, schema, []string{})
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
		// Use "public" schema for PostgreSQL
		schema := "public"

		// Skip the tags table
		skipTables := []string{"tags"}
		insertStmts, err := getInsertsStmts(ctx, db, schema, skipTables)
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
			sql:      "INSERT INTO users (name, email) VALUES ($1, $2)",
			action:   "create",
			expected: "-- name: CreateUser :one",
		},
		{
			name:     "Read post comment",
			sql:      "SELECT * FROM posts WHERE id = $1",
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
