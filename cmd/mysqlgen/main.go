package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/miyataka/sqlgen"
	"github.com/spf13/cobra"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	_ "github.com/go-sql-driver/mysql"
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
		dsn, dbname, err := sqlgen.ParseMysqlDSN(dsn)
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

func genComment4Sqlc(stmt string) string {
	tn, err := getTableName(stmt)
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("-- Create%s :exec", SnakeToPascal(tn))
}

// GetTableName extracts the table name from an INSERT SQL statement.
func getTableName(sql string) (string, error) {
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
