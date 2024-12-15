package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

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
		parsed, err := sqlgen.ParseDSN(dsn)
		if err != nil {
			log.Fatal(err)
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		database, ok := parsed["database"]
		if !ok {
			log.Fatal("database is required")
		}

		// execute query
		insertStmts, err := getInsertsStmts(ctx, db, database)
		if err != nil {
			log.Fatal(err)
		}
		for _, stmt := range insertStmts {
			str := ""
			if sqlc {
				str += fmt.Sprintf("%s\n", genComment4Sqlc(stmt))
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

func genComment4Sqlc(stmt string) string {
	tn, err := sqlgen.GetTableName(stmt)
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("-- Create%s :one", sqlgen.SnakeToPascal(tn))
}
