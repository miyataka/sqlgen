package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/miyataka/sqlgen"
	"github.com/spf13/cobra"
)

var (
	dsn  string
	sqlc bool
)

func main() {
	rootCmd.Flags().StringVarP(&dsn, "dsn", "d", "", "DSN e.g. postgres://user:password@localhost:5432/test")
	rootCmd.Flags().BoolVar(&sqlc, "sqlc", false, "generate comment for sqlc")
	err := rootCmd.MarkFlagRequired("dsn")
	if err != nil {
		log.Fatal(err)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "psqlgen",
	Short: "psqlgen is a sql generator",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		database, err := getDatabaseFromDsn(dsn)
		if err != nil {
			log.Fatal(err)
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		// execute query
		insertStmts, err := getInsertsStmts(ctx, db, database)
		if err != nil {
			log.Fatal(err)
		}
		for _, stmt := range insertStmts {
			str := ""
			if sqlc {
				str += fmt.Sprintf("%s\n", genComment4Sqlc(stmt, "create"))
			}
			str += stmt + "\n"
			if sqlc {
				str += "\n"
			}
			fmt.Print(str)
		}

		selectStmts, err := getSelectByPkStmts(ctx, db, database)
		if err != nil {
			log.Fatal(err)
		}
		for _, stmt := range selectStmts {
			str := ""
			if sqlc {
				str += fmt.Sprintf("%s\n", genComment4Sqlc(stmt, "read"))
			}
			str += stmt + "\n"
			if sqlc {
				str += "\n"
			}
			fmt.Print(str)
		}
	},
}

const generatePostgresInserts = `
WITH column_with_placeholders AS (
    SELECT
        c.table_schema,
        c.table_name,
        c.column_name,
        c.ordinal_position,
        '$' || ROW_NUMBER() OVER (PARTITION BY c.table_schema, c.table_name ORDER BY c.ordinal_position) AS placeholder
    FROM
        information_schema.columns c
    WHERE
        c.table_schema = $1 -- schema name
        AND (c.column_default IS NULL OR NOT c.column_default LIKE 'nextval(%'::text ) -- sequenceを除外
)
SELECT
    'INSERT INTO ' || table_name ||
    ' (' || string_agg(column_name, ', ' ORDER BY ordinal_position) || ') ' ||
    'VALUES (' || string_agg(placeholder, ', ' ORDER BY ordinal_position) || ') RETURNING *;' AS insert_statement
FROM
    column_with_placeholders
GROUP BY
    table_schema, table_name
ORDER BY
    table_name;
`

func getInsertsStmts(ctx context.Context, db *sql.DB, database string) ([]string, error) {
	rows, err := db.QueryContext(ctx, generatePostgresInserts, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var insertStatements []string
	for rows.Next() {
		var insertStatement string
		if err := rows.Scan(&insertStatement); err != nil {
			return nil, err
		}
		insertStatements = append(insertStatements, insertStatement)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return insertStatements, nil
}

func getSelectByPkStmts(ctx context.Context, db *sql.DB, database string) ([]string, error) {
	rows, err := db.QueryContext(ctx, generatePostgresSelectByPk, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var selectStatements []string
	for rows.Next() {
		var selectStatement string
		if err := rows.Scan(&selectStatement); err != nil {
			return nil, err
		}
		selectStatements = append(selectStatements, selectStatement)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return selectStatements, nil
}

const generatePostgresSelectByPk = `
WITH column_list AS (
    SELECT
        c.table_schema,
        c.table_name,
        c.column_name,
        c.ordinal_position
    FROM
        information_schema.columns c
    WHERE
        c.table_schema = $1 -- schema name
),
primary_keys AS (
    SELECT
        kcu.table_schema,
        kcu.table_name,
        kcu.column_name,
        ROW_NUMBER() OVER (PARTITION BY kcu.table_schema, kcu.table_name ORDER BY kcu.column_name) AS placeholder_number
    FROM
        information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        AND tc.table_name = kcu.table_name
    WHERE
        tc.constraint_type = 'PRIMARY KEY'
        AND kcu.table_schema = $1 -- schema name
),
where_pk AS (
        SELECT
            pk.table_schema,
            pk.table_name,
            string_agg(pk.column_name || ' = $' || pk.placeholder_number, ' AND ') AS cond
        FROM primary_keys pk
        GROUP BY pk.table_schema, pk.table_name
)
SELECT
    'SELECT ' || string_agg(cl.column_name, ', ' ORDER BY cl.ordinal_position) ||
    ' FROM ' || cl.table_name ||
    ' WHERE ' || pk.cond ||
    ';' AS select_statement
FROM
    column_list cl INNER JOIN where_pk pk ON cl.table_schema = pk.table_schema AND cl.table_name = pk.table_name
GROUP BY
    cl.table_schema, cl.table_name, pk.cond
ORDER BY cl.table_name;
`

func genComment4Sqlc(stmt string, action string) string {
	tn, err := sqlgen.GetTableName(stmt)
	if err != nil {
		log.Fatal(err)
	}
	tableName := sqlgen.SnakeToPascal(sqlgen.Singularize(tn))
	switch action {
	case "create":
		return fmt.Sprintf("-- name: Create%s :one", tableName)
	case "read":
		return fmt.Sprintf("-- name: Get%sByPk :one", tableName)
	default:
		panic("invalid action")
	}
}

// getDatabaseFromDsn parses a DSN string and returns the database name.
func getDatabaseFromDsn(dsn string) (string, error) {
	parsed, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("failed to parse DSN: %w", err)
	}
	return parsed.Database, nil
}
