package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/miyataka/sqlgen"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "psqlgen",
	Short: "psqlgen is a sql generator",
	Run: func(cmd *cobra.Command, args []string) {
		// TODO flag for sqlc
		dsn := "postgres://localhost:5432/test" // TODO dsn from flag
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
		rows, err := db.QueryContext(ctx, generatePostgresInserts, database)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var insertStatement string
			if err := rows.Scan(&insertStatement); err != nil {
				log.Fatal(err)
			}
			fmt.Println(insertStatement)
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
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
