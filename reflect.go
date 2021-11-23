package gomysql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ErrNoMatchLineFound no match line found
var ErrNoMatchLineFound = errors.New("go-mysql: no match line found")

// ScanColumnNameToStructName when query scanning, name conversion
var ScanColumnNameToStructName = func(name string) string {
	return UnderlineToPascal(strings.ToLower(name))
}

// ScanOne any type: *AnyStruct
func ScanOne(any interface{}, rows *sql.Rows) (err error) {
	prt := reflect.TypeOf(any)
	if prt.Kind() != reflect.Ptr {
		err = errors.New("go-mysql: `any` is not pointer")
		return
	}
	rt := prt.Elem()
	if rt.Kind() != reflect.Struct {
		err = errors.New("go-mysql: `any` is not struct pointer")
		return
	}
	if !rows.Next() {
		err = ErrNoMatchLineFound
		return
	}
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	var field reflect.Value
	var index int
	var column string
	line := reflect.Indirect(reflect.New(rt))
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	for index, column = range columns {
		field = line.FieldByName(ScanColumnNameToStructName(column))
		if field == zero {
			err = fmt.Errorf("struct field `%s` does not match", column)
			return
		}
		if !field.CanSet() {
			err = fmt.Errorf("struct field `%s` cannot set value", column)
			return
		}
		scanner[index] = field.Addr().Interface()
	}
	err = rows.Scan(scanner...)
	if err != nil {
		return
	}
	reflect.ValueOf(any).Elem().Set(line)
	return
}

// ScanAll any type: *[]AnyStruct | *[]*AnyStruct
func ScanAll(any interface{}, rows *sql.Rows) (err error) {
	prt := reflect.TypeOf(any)
	if prt.Kind() != reflect.Ptr {
		err = errors.New("go-mysql: `any` is not pointer")
		return
	}
	prt1 := prt.Elem()
	if prt1.Kind() != reflect.Slice {
		err = errors.New("go-mysql: `any` is not slice pointer")
		return
	}
	prt2 := prt1.Elem()
	if prt2.Kind() == reflect.Ptr {
		prt3 := prt2.Elem()
		if prt3.Kind() == reflect.Struct {
			// slice element is struct pointer
			err = ScanAll2(any, rows)
		} else {
			err = errors.New("go-mysql: `any` slice element is neither a struct nor a struct pointer")
			return
		}
	} else if prt2.Kind() == reflect.Struct {
		// slice element is struct
		err = ScanAll1(any, rows)
	} else {
		err = errors.New("go-mysql: `any` slice element is neither a struct nor a struct pointer")
		return
	}
	return
}

// ScanAll1 any type: *[]AnyStruct, it is recommended to call ScanAll
func ScanAll1(any interface{}, rows *sql.Rows) (err error) {
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	var lines reflect.Value
	var values reflect.Value
	var field reflect.Value
	var index int
	var column string
	at := reflect.TypeOf(any)
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	for rows.Next() {
		lines = reflect.New(at.Elem().Elem())
		values = reflect.Indirect(lines)
		for index, column = range columns {
			field = values.FieldByName(ScanColumnNameToStructName(column))
			if zero == field {
				err = fmt.Errorf("struct field `%s` does not match", column)
				return
			}
			if !field.CanSet() {
				err = fmt.Errorf("struct field `%s` cannot set value", column)
				return
			}
			scanner[index] = field.Addr().Interface()
		}
		err = rows.Scan(scanner...)
		if err != nil {
			return
		}
		slices = reflect.Append(slices, lines.Elem())
	}
	if slices.Len() == 0 {
		err = ErrNoMatchLineFound
	}
	reflect.ValueOf(any).Elem().Set(slices)
	return
}

// ScanAll2 any type: *[]*AnyStruct, it is recommended to call ScanAll
func ScanAll2(any interface{}, rows *sql.Rows) (err error) {
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	var lines reflect.Value
	var values reflect.Value
	var field reflect.Value
	var index int
	var column string
	at := reflect.TypeOf(any)
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	for rows.Next() {
		lines = reflect.New(at.Elem().Elem().Elem())
		values = reflect.Indirect(lines)
		for index, column = range columns {
			field = values.FieldByName(ScanColumnNameToStructName(column))
			if zero == field {
				err = fmt.Errorf("struct field `%s` does not match", column)
				return
			}
			if !field.CanSet() {
				err = fmt.Errorf("struct field `%s` cannot set value", column)
				return
			}
			scanner[index] = field.Addr().Interface()
		}
		err = rows.Scan(scanner...)
		if err != nil {
			return
		}
		slices = reflect.Append(slices, lines)
	}
	if slices.Len() == 0 {
		err = ErrNoMatchLineFound
	}
	reflect.ValueOf(any).Elem().Set(slices)
	return
}
