package sqlhelp

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/athomecomar/storeql/name"
	"github.com/jmoiron/sqlx"
)

// MockDB wraps sqlmock.New with sqlx to return *sqlx.DB with sqlmock.Mock
func MockDB(t *testing.T) (db *sqlx.DB, mock sqlmock.Sqlmock) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could connect to database: %v", err)
	}
	db = sqlx.NewDb(mockDB, "sqlmock")
	db.MapperFunc(name.ToSnakeCase)
	return db, mock
}
