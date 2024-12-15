package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/miyataka/sqlgen"
	"github.com/spf13/cobra"

	"github.com/go-sql-driver/mysql"
)

var (
	dsn  string
	sqlc bool
)

func main() {
	rootCmd.Flags().StringVarP(&dsn, "dsn", "d", "", "DSN e.g. user:password@tcp(localhost:5432)/test")
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
	Use:   "mysqlgen",
	Short: "mysqlgen is a sql generator",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		dbname, err := getDatabaseFromDsn(dsn)
		if err != nil {
			log.Fatal(err)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		// execute query
		insertStmts, err := getInsertsStmts(ctx, db, dbname)
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

		selectStmts, err := getSelectsStmts(ctx, db, dbname)
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

func genComment4Sqlc(stmt string, action string) string {
	tn, err := sqlgen.GetTableName(stmt)
	if err != nil {
		log.Fatal(err)
	}
	tableName := sqlgen.SnakeToPascal(sqlgen.Singularize(tn))
	switch action {
	case "create":
		return fmt.Sprintf("-- name: Create%s :exec", tableName)
	case "read":
		return fmt.Sprintf("-- name: Get%sByPk :one", tableName)
	default:
		panic("invalid action")
	}
}

const generateMysqlInserts = `
SELECT
    CONCAT(
        'INSERT INTO ', table_name,
        ' (', GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ', '), ') ',
        'VALUES (', GROUP_CONCAT('?' ORDER BY ordinal_position SEPARATOR ', '), ');'
    ) AS insert_statement
FROM (
    SELECT
        c.TABLE_SCHEMA AS table_schema,
        c.TABLE_NAME AS table_name,
        c.COLUMN_NAME AS column_name,
        c.ORDINAL_POSITION AS ordinal_position
    FROM
        INFORMATION_SCHEMA.COLUMNS c
    WHERE
        c.TABLE_SCHEMA = ? -- schema/database name
        AND c.EXTRA NOT LIKE '%auto_increment%' -- except AUTO_INCREMENT columns
    ORDER BY
        c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
) subquery
GROUP BY
    table_schema, table_name
ORDER BY
    table_name;
`

func getInsertsStmts(ctx context.Context, db *sql.DB, database string) ([]string, error) {
	rows, err := db.QueryContext(ctx, generateMysqlInserts, database)
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

const generateMysqlSelects = `
SELECT
    CONCAT(
        'SELECT ', GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ', '),
        ' FROM ', table_name,
        ' WHERE ', GROUP_CONCAT(
            CONCAT(pk_column, ' = ?') ORDER BY pk_ordinal_position SEPARATOR ' AND '
        ), ';'
    ) AS select_statement
FROM (
    SELECT
        c.TABLE_SCHEMA AS table_schema,
        c.TABLE_NAME AS table_name,
        c.COLUMN_NAME AS column_name,
        c.ORDINAL_POSITION AS ordinal_position,
        kcu.COLUMN_NAME AS pk_column,
        kcu.ORDINAL_POSITION AS pk_ordinal_position
    FROM
        INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON c.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND c.TABLE_NAME = kcu.TABLE_NAME
        AND c.COLUMN_NAME = kcu.COLUMN_NAME
        AND kcu.CONSTRAINT_NAME = 'PRIMARY'
    WHERE
        c.TABLE_SCHEMA = ? -- schema/database name
    ORDER BY
        c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
) subquery
GROUP BY
    table_schema, table_name
ORDER BY
    table_name;
`

func getSelectsStmts(ctx context.Context, db *sql.DB, database string) ([]string, error) {
	rows, err := db.QueryContext(ctx, generateMysqlSelects, database)
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

func getDatabaseFromDsn(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("failed to parse DSN: %w", err)
	}
	return cfg.DBName, nil
}
