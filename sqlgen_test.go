package sqlgen

import "testing"

func TestGetTableName(t *testing.T) {
	sql := "INSERT INTO users (id, name) VALUES ($1, $2)"
	tableName, err := GetTableName(sql)
	if err != nil {
		t.Errorf("GetTableName failed: %v", err)
	}
	if tableName != "users" {
		t.Errorf("GetTableName failed: %v", tableName)
	}

	sql = "SELECT id, name FROM users"
	tableName, err = GetTableName(sql)
	if err != nil {
		t.Errorf("GetTableName failed: %v", err)
	}
	if tableName != "users" {
		t.Errorf("GetTableName failed: %v", tableName)
	}
}
