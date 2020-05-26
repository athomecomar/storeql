package sqltest

import (
	"database/sql/driver"
	"reflect"
	"sort"
	"testing"

	"github.com/athomecomar/storeql"
	"github.com/athomecomar/storeql/name"
	"github.com/gedex/inflector"
	"github.com/google/go-cmp/cmp"
)

func TestSQLColumns(t *testing.T, s storeql.Storable, message string) {
	typeOf := reflect.TypeOf(s)
	var want []string
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		col := name.ToSnakeCase(field.Name)
		want = append(want, col)
	}
	sort.Strings(want)

	got := storeql.SQLColumns(s)
	sort.Strings(got)
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("%s.SQLColumns() mismatch (-want +got): %s", message, diff)
	}
}

func TestSQLMap(t *testing.T, s storeql.Storable, message string) {
	valueOf := reflect.ValueOf(s)
	typeOf := reflect.TypeOf(s)

	wantMap := make(map[string]driver.Value)
	for i := 0; i < typeOf.NumField(); i++ {
		fieldType := typeOf.Field(i)
		fieldValue := valueOf.Field(i)
		wantCol := name.ToSnakeCase(fieldType.Name)

		wantMap[wantCol] = fieldValue
	}

	sqlMap := s.SQLMap()
	if diff := cmp.Diff(sqlMap, wantMap); diff != "" {
		t.Errorf("%s.SQLMap() mismatch (-want +got): %s", message, diff)
	}
}

func TestSQLTable(t *testing.T, s storeql.Storable, message string) {
	typeOf := reflect.TypeOf(s)
	want := inflector.Pluralize(name.ToSnakeCase(typeOf.Name()))
	got := s.SQLTable()
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("%s.SQLTable() mismatch (-want +got): %s", message, diff)
	}
}
