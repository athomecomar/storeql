package storeql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"

	"github.com/athomecomar/storeql/name"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// Storable entity is any entity that can be stored in an SQL database
type Storable interface {
	GetId() uint64
	SetId(uint64)
	SQLTable() string
	SQLMap() map[string]driver.Value
}

func SQLColumns(s Storable) (cols []string) {
	for key := range s.SQLMap() {
		cols = append(cols, key)
	}
	sort.Strings(cols)
	return
}

func SQLValues(s Storable) (vals []driver.Value) {
	sqlMap := s.SQLMap()
	cols := SQLColumns(s)

	for _, col := range cols {
		vals = append(vals, sqlMap[col])
	}
	return
}

func sqlColumnValuesWithoutId(storable Storable) string {
	fValues := []string{}
	for _, s := range SQLColumns(storable) {
		if s == "id" {
			continue
		}
		fValues = append(fValues, ":"+s)
	}
	return name.Parenthize(strings.Join(fValues, ","))
}

func sqlNamedColumnValues(storable Storable) string {
	fValues := []string{}
	for _, s := range SQLColumns(storable) {
		fValues = append(fValues, fmt.Sprintf("%s=:%s", s, s))
	}
	return strings.Join(fValues, ", ")
}

func sqlNamedColumnValuesWithoutId(storable Storable) string {
	fValues := []string{}
	for _, s := range SQLColumns(storable) {
		if s == "id" {
			continue
		}
		fValues = append(fValues, fmt.Sprintf("%s=:%s", s, s))
	}
	return strings.Join(fValues, ", ")
}

func sqlColumnValues(storable Storable) string {
	fValues := []string{}
	for _, s := range SQLColumns(storable) {
		fValues = append(fValues, ":"+s)
	}
	return name.Parenthize(strings.Join(fValues, ","))
}

func sqlColumnNamesWithoutId(storable Storable) string {
	return name.Parenthize(strings.ReplaceAll(strings.Join(SQLColumns(storable), ","), "id,", ""))
}

func sqlColumnNames(storable Storable) string {
	return name.Parenthize(strings.Join(SQLColumns(storable), ","))
}

func Where(ctx context.Context, db *sqlx.DB, storable Storable, whereClause string, args ...interface{}) *sqlx.Row {
	return db.QueryRowxContext(ctx, `SELECT * FROM `+storable.SQLTable()+` WHERE `+whereClause, args...)
}

func WhereMany(ctx context.Context, db *sqlx.DB, storable Storable, whereClause string, args ...interface{}) (*sqlx.Rows, error) {
	rows, err := db.QueryxContext(ctx, `SELECT * FROM `+storable.SQLTable()+` WHERE `+whereClause, args...)
	if err != nil {
		return nil, errors.Wrap(err, "QueryxContext")
	}
	return rows, nil
}

func UpsertIntoDB(ctx context.Context, db *sqlx.DB, storables ...Storable) error {
	var inserts, updates []Storable
	for _, store := range storables {
		if store.GetId() == 0 {
			inserts = append(inserts, store)
		} else {
			updates = append(updates, store)
		}
	}
	err := UpdateIntoDB(ctx, db, updates...)
	if err != nil {
		return errors.Wrap(pqErr(err), "update into db")
	}
	err = InsertIntoDB(ctx, db, inserts...)
	if err != nil {
		return errors.Wrap(pqErr(err), "insert into db")
	}
	return nil
}

func UpdateIntoDB(ctx context.Context, db *sqlx.DB, storables ...Storable) error {
	qtToStore := len(storables)
	if qtToStore == 0 {
		return nil
	}

	ref := storables[0] // takes it as a reference for all entities given
	qr := execBoilerplate("UPDATE", ref)
	rows, err := db.NamedExecContext(ctx, qr, storables)
	if err != nil {
		return errors.Wrap(pqErr(err), "named exec ctx")
	}
	rowsQt, err := rows.RowsAffected()
	if err != nil {
		return errors.Wrap(pqErr(err), "rows affected")
	}
	if int(rowsQt) != qtToStore {
		return pqErr(errMismatchAffectedRows)
	}
	return nil
}

var errNilStorableEntity = errors.New("nil storable entity")
var errMismatchAffectedRows = errors.New("the affected rows quantity does not match with the given storables")

// InsertIntoDB inserts the storable entity to the DB
// Finally, it assigns the inserted Id to the given entities
func InsertIntoDB(ctx context.Context, db *sqlx.DB, storables ...Storable) error {
	if len(storables) == 0 {
		return pqErr(errNilStorableEntity)
	}
	ref := storables[0] // takes it as a schema reference for all entities given
	qr := execBoilerplate("INSERT INTO", ref) + " RETURNING id"
	ids, err := db.NamedQueryContext(ctx, qr, storables)
	if err != nil {
		return errors.Wrap(pqErr(err), "named query ctx")
	}
	defer ids.Close()
	var i int
	for ids.Next() {
		var id uint64
		err := ids.Scan(&id)
		if err != nil {
			return errors.Wrap(pqErr(err), "id scan")
		}
		storables[i].SetId(id)
		i += 1
	}
	err = ids.Err()
	if err != nil {
		return errors.Wrap(pqErr(err), "cursor err")
	}

	return nil
}

func DeleteFromDB(ctx context.Context, db *sqlx.DB, storables ...Storable) error {
	if len(storables) == 0 {
		return pqErr(errNilStorableEntity)
	}
	for _, storable := range storables {
		if storable.GetId() == 0 {
			return ErrNoId
		}
	}

	ref := storables[0]

	if ctx == nil {
		ctx = context.Background()
	}

	_, err := db.NamedExecContext(
		ctx,
		`DELETE FROM `+ref.SQLTable()+` WHERE id=:id`,
		storables,
	)
	if err != nil {
		return errors.Wrap(pqErr(err), "named exec ctx")
	}

	return nil
}

func execBoilerplate(action string, storable Storable) (boilerplate string) {
	switch action {
	case "INSERT INTO":
		boilerplate = action + ` ` + storable.SQLTable() + ` ` + sqlColumnNamesWithoutId(storable) + ` VALUES ` + sqlColumnValuesWithoutId(storable)
	case "UPDATE":
		boilerplate = action + ` ` + storable.SQLTable() + ` SET ` + sqlNamedColumnValuesWithoutId(storable) + ` WHERE id=:id`
	}
	return
}
