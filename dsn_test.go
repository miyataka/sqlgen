package sqlgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want map[string]string
	}{
		{
			"postgresql valid dsn",
			"postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full",
			map[string]string{
				"protocol": "postgres",
				"host":     "localhost",
				"user":     "pqgotest",
				"password": "password",
				"database": "pqgotest",
				"sslmode":  "verify-full",
			},
		},
		{
			"mysql valid dsn",
			"mysql://user:password@localhost:3306/exampledb?charset=utf8&parseTime=True&loc=Local",
			map[string]string{
				"protocol":  "mysql",
				"host":      "localhost",
				"port":      "3306",
				"user":      "user",
				"password":  "password",
				"database":  "exampledb",
				"charset":   "utf8",
				"parseTime": "True",
				"loc":       "Local",
			},
		},
		// TODO sqlite3 dsn
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDSN(tt.dsn)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
