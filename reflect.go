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
	var index int
	var column string
	for index, column = range columns {
		columns[index] = ScanColumnNameToStructName(column)
	}
	var field reflect.Value
	line := reflect.Indirect(reflect.New(rt))
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	cols := map[string]int{}
	for i := 0; i < line.NumField(); i++ {
		cols[line.Type().Field(i).Name] = i
	}
	for index, column = range columns {
		field = line.Field(cols[column])
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
	var index int
	var column string
	for index, column = range columns {
		columns[index] = ScanColumnNameToStructName(column)
	}
	var line reflect.Value
	var value reflect.Value
	var field reflect.Value
	at := reflect.TypeOf(any)
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	lines := reflect.Indirect(reflect.New(at.Elem().Elem()))
	cols := map[string]int{}
	for i := 0; i < lines.NumField(); i++ {
		cols[lines.Type().Field(i).Name] = i
	}
	for rows.Next() {
		line = reflect.New(at.Elem().Elem())
		value = reflect.Indirect(line)
		for index, column = range columns {
			field = value.Field(cols[column])
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
		slices = reflect.Append(slices, line.Elem())
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
	var index int
	var column string
	for index, column = range columns {
		columns[index] = ScanColumnNameToStructName(column)
	}
	var line reflect.Value
	var value reflect.Value
	var field reflect.Value
	at := reflect.TypeOf(any)
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	lines := reflect.Indirect(reflect.New(at.Elem().Elem().Elem()))
	cols := map[string]int{}
	for i := 0; i < lines.NumField(); i++ {
		cols[lines.Type().Field(i).Name] = i
	}
	for rows.Next() {
		line = reflect.New(at.Elem().Elem().Elem())
		value = reflect.Indirect(line)
		for index, column = range columns {
			field = value.Field(cols[column])
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
		slices = reflect.Append(slices, line)
	}
	if slices.Len() == 0 {
		err = ErrNoMatchLineFound
	}
	reflect.ValueOf(any).Elem().Set(slices)
	return
}
