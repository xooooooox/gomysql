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

// ReflectColumnNameToStructName when query scanning, name conversion
var ReflectColumnNameToStructName = func(name string) string {
	return UnderlineToPascal(strings.ToLower(name))
}

// ReflectOne any type: *AnyStruct
func ReflectOne(rows *sql.Rows, any interface{}) (err error) {
	prt := reflect.TypeOf(any)
	if prt.Kind() != reflect.Ptr {
		err = errors.New("`any` is not pointer")
		return
	}
	rt := prt.Elem()
	if rt.Kind() != reflect.Struct {
		err = errors.New("`any` is not struct pointer")
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
		field = line.FieldByName(ReflectColumnNameToStructName(column))
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

// ReflectAll any type: *[]AnyStruct
func ReflectAll(rows *sql.Rows, any interface{}) (err error) {
	prt := reflect.TypeOf(any)
	if prt.Kind() != reflect.Ptr {
		err = errors.New("`any` is not pointer")
		return
	}
	srt := prt.Elem()
	if srt.Kind() != reflect.Slice {
		err = errors.New("`any` is not slice pointer")
		return
	}
	trt := srt.Elem()
	if trt.Kind() != reflect.Struct {
		err = errors.New("`any` slice element is not struct")
		return
	}
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
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	for rows.Next() {
		lines = reflect.New(trt)
		values = reflect.Indirect(lines)
		for index, column = range columns {
			field = values.FieldByName(ReflectColumnNameToStructName(column))
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

// ReflectAllPointer any type: *[]*AnyStruct
func ReflectAllPointer(rows *sql.Rows, any interface{}) (err error) {
	prt := reflect.TypeOf(any)
	if prt.Kind() != reflect.Ptr {
		err = errors.New("`any` is not pointer")
		return
	}
	srt := prt.Elem()
	if srt.Kind() != reflect.Slice {
		err = errors.New("`any` is not slice pointer")
		return
	}
	crt := srt.Elem()
	if crt.Kind() != reflect.Ptr {
		err = errors.New("`any` slice element is not pointer")
		return
	}
	trt := crt.Elem()
	if trt.Kind() != reflect.Struct {
		err = errors.New("`any` slice element is not struct pointer")
		return
	}
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
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	zero := reflect.Value{}
	for rows.Next() {
		lines = reflect.New(trt)
		values = reflect.Indirect(lines)
		for index, column = range columns {
			field = values.FieldByName(ReflectColumnNameToStructName(column))
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
