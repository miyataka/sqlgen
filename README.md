# SQLGen
[![Go Reference](https://pkg.go.dev/badge/github.com/miyataka/sqlgen.svg)](https://pkg.go.dev/github.com/miyataka/sqlgen)

SQLGen is a command-line tool written in Go that helps you generate SQL queries efficiently.

## Features

- Generate basic SQL queries for PostgreSQL and MySQL

## Installation

To install SQLGen, you need to have Go installed on your machine. Then, you can use the following command to install SQLGen:

```sh
# postgres
go install github.com/miyataka/sqlgen/cmd/psqlgen@latest

# mysql
go install github.com/miyataka/sqlgen/cmd/mysqlgen@latest
```

## Usage

After installing SQLGen, you can use it from the command line. Here are some examples of how to use SQLGen:

```sh
# Generate SQL queries for PostgreSQL
psqlgen --dsn="postgres://user:password@localhost:5432/dbname"

# Generate SQL queries for MySQL
mysqlgen --dsn= "user:password@tcp(localhost:3306)/dbname"
```

When using with sqlc, you can generate comment for sqlc with just `--sqlc` flag:

```sh
# Generate SQL queries for PostgreSQL with comments for sqlc
psqlgen --dsn="postgres://user:password@localhost:5432/dbname" --sqlc

# Generate SQL queries for MySQL with comments for sqlc
mysqlgen --dsn= "user:password@tcp(localhost:3306)/dbname" --sqlc
```

## Contributing

We welcome contributions to SQLGen! If you have any ideas, suggestions, or bug reports, please open an issue or submit a pull request on GitHub.

## License

This project is licensed under the MIT License.
