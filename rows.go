package instrumentedsql

import (
	"context"
	"database/sql/driver"
	"io"
	"reflect"
	"time"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Rows                           = wrappedRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = wrappedRows{}
	_ driver.RowsColumnTypeLength           = wrappedRows{}
	_ driver.RowsColumnTypeNullable         = wrappedRows{}
	_ driver.RowsColumnTypePrecisionScale   = wrappedRows{}
	_ driver.RowsColumnTypeScanType         = wrappedRows{}
	_ driver.RowsNextResultSet              = wrappedRows{}
)

type wrappedRows struct {
	opts
	ctx    context.Context
	parent driver.Rows
}

func (r wrappedRows) Columns() []string {
	return r.parent.Columns()
}

func (r wrappedRows) Close() error {
	return r.parent.Close()
}

func (r wrappedRows) Next(dest []driver.Value) (err error) {
	if !r.hasOpExcluded(OpSQLRowsNext) {
		span := r.GetSpan(r.ctx).NewChild(OpSQLRowsNext)
		span.SetLabel("component", "database/sql")
		defer func() {
			if err != io.EOF {
				span.SetError(err)
			}
			span.Finish()
		}()
		start := time.Now()
		defer func() {
			r.Log(r.ctx, OpSQLRowsNext, "err", err, "duration", time.Since(start))
		}()
	}

	return r.parent.Next(dest)
}

func (r wrappedRows) ColumnTypeDatabaseTypeName(index int) string {
	if ct, ok := r.parent.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return ct.ColumnTypeDatabaseTypeName(index)
	}
	return ""
}

func (r wrappedRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if ct, ok := r.parent.(driver.RowsColumnTypeLength); ok {
		return ct.ColumnTypeLength(index)
	}
	return 0, false
}

func (r wrappedRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if ct, ok := r.parent.(driver.RowsColumnTypeNullable); ok {
		return ct.ColumnTypeNullable(index)
	}
	return false, false
}

func (r wrappedRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if ct, ok := r.parent.(driver.RowsColumnTypePrecisionScale); ok {
		return ct.ColumnTypePrecisionScale(index)
	}
	return 0, 0, false
}

func (r wrappedRows) ColumnTypeScanType(index int) reflect.Type {
	if ct, ok := r.parent.(driver.RowsColumnTypeScanType); ok {
		return ct.ColumnTypeScanType(index)
	}
	return nil
}

func (r wrappedRows) HasNextResultSet() bool {
	if nr, ok := r.parent.(driver.RowsNextResultSet); ok {
		return nr.HasNextResultSet()
	}
	return false
}

func (r wrappedRows) NextResultSet() error {
	if nr, ok := r.parent.(driver.RowsNextResultSet); ok {
		return nr.NextResultSet()
	}
	return driver.ErrSkip
}

