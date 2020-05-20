package storeql

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/athomecomar/storeql/test/sqlassist"
	"github.com/athomecomar/storeql/test/sqlhelp"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

var errFoo = errors.New("foo")

func TestInsertIntoDB(t *testing.T) {
	type args struct {
		ctx       context.Context
		storables []Storable
	}
	tests := []struct {
		name          string
		args          args
		wantErr       error
		wantStorables []Storable
		stub          *sqlassist.QueryStubber
	}{
		{
			name: "successfully single insert",
			args: args{
				storables: []Storable{&storableStub{Name: "foo"}},
			},
			wantStorables: []Storable{&storableStub{Name: "foo", Id: 10}},
			stub: &sqlassist.QueryStubber{
				Expect: "INSERT INTO entities_stub", Rows: sqlmock.NewRows([]string{"id"}).AddRow(10),
			},
		},
		{
			name: "successfully multi insert",
			args: args{
				storables: []Storable{&storableStub{Name: "foo"}, &storableStub{Name: "foo"}},
			},
			wantStorables: []Storable{&storableStub{Name: "foo", Id: 10}, &storableStub{Name: "foo", Id: 20}},
			stub: &sqlassist.QueryStubber{
				Expect: "INSERT INTO entities_stub", Rows: sqlmock.NewRows([]string{"id"}).AddRow(10).AddRow(20),
			},
		},

		{
			name: "exec returns err",
			args: args{
				storables: []Storable{&storableStub{}},
			},
			wantStorables: []Storable{&storableStub{}},
			wantErr:       errFoo,
			stub:          &sqlassist.QueryStubber{Expect: "INSERT INTO entities_stub", Err: errFoo},
		},
		{
			name:    "nil interface given",
			args:    args{},
			wantErr: errNilStorableEntity,
			stub:    &sqlassist.QueryStubber{Expect: "INSERT INTO entities_stub", Err: errFoo},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldStorables := tt.args.storables
			db, mock := sqlhelp.MockDB(t)
			tt.stub.Stub(mock)
			if tt.args.ctx == nil {
				tt.args.ctx = context.Background()
			}
			err := InsertIntoDB(tt.args.ctx, db, tt.args.storables...)
			if errors.Cause(err) != errors.Cause(tt.wantErr) {
				t.Errorf("InsertIntoDB() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				if diff := cmp.Diff(oldStorables, tt.args.storables); diff != "" {
					t.Errorf("InsertIntoDB() errored mismatch (-want +got): %s", diff)
				}
				return
			}
			if diff := cmp.Diff(tt.wantStorables, tt.args.storables); diff != "" {
				t.Errorf("InsertIntoDB() mismatch (-want +got): %s", diff)
			}
		})
	}
}

func TestUpdateIntoDB(t *testing.T) {
	type args struct {
		ctx       context.Context
		storables []Storable
	}
	tests := []struct {
		name          string
		args          args
		wantErr       error
		wantStorables []Storable
		stub          *sqlassist.ExecStubber
	}{
		{
			name:    "update returns err",
			wantErr: errFoo,
			args: args{
				storables: []Storable{&storableStub{}},
			},
			stub: &sqlassist.ExecStubber{Expect: "UPDATE entities_stub", Result: sqlmock.NewErrorResult(errFoo)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// oldStorable := tt.args.storable
			db, mock := sqlhelp.MockDB(t)
			tt.stub.Stub(mock)
			if tt.args.ctx == nil {
				tt.args.ctx = context.Background()
			}
			err := UpdateIntoDB(tt.args.ctx, db, tt.args.storables...)
			if errors.Cause(err) != errors.Cause(tt.wantErr) {
				t.Errorf("UpdateIntoDB() error = %v, wantErr %v", err, tt.wantErr)
			}

		})
	}
}

type storableStub struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

func (s *storableStub) GetId() int64 {
	return s.Id
}

func (s *storableStub) SetId(id int64) {
	s.Id = id
}

func (s *storableStub) SQLTable() string {
	return "entities_stub"
}

func (s *storableStub) SQLColumns() []string {
	return []string{
		"id",
		"name",
	}
}

func Test_execBoilerplate(t *testing.T) {
	type args struct {
		action   string
		storable Storable
	}
	tests := []struct {
		name            string
		args            args
		wantBoilerplate string
	}{
		{
			name:            "update basic",
			args:            args{action: "UPDATE", storable: &storableStub{}},
			wantBoilerplate: "UPDATE entities_stub SET name=:name WHERE id=:id",
		},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotBoilerplate := execBoilerplate(tt.args.action, tt.args.storable); gotBoilerplate != tt.wantBoilerplate {
				t.Errorf("execBoilerplate() = %v, want %v", gotBoilerplate, tt.wantBoilerplate)
			}
		})
	}
}
