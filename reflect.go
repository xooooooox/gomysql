package gomysql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ErrNoMatchLineFound no match line found.
var ErrNoMatchLineFound = errors.New("go-mysql: no match line found")

// defaultColumnNameToStructAttributeName when query scanning, database table column name change to go struct attribute name.
var defaultColumnNameToStructAttributeName = func(name string) string {
	return UnderlineToPascal(strings.ToLower(name))
}

// SetColumnNameToStructAttributeName when query scanning, database table column name change to go struct attribute name.
func SetColumnNameToStructAttributeName(name func(name string) string) {
	defaultColumnNameToStructAttributeName = name
}

// defaultStructAttributeNameToColumnName when execute insert, go struct attribute name change to database table column name.
var defaultStructAttributeNameToColumnName = func(name string) string {
	return PascalToUnderline(name)
}

// SetStructAttributeNameToColumnName when execute insert, go struct attribute name change to database table column name.
func SetStructAttributeNameToColumnName(name func(name string) string) {
	defaultStructAttributeNameToColumnName = name
}

//
// Deprecated: Use Scanning instead.
//
// ScanOne any type: *AnyStruct.
func ScanOne(any interface{}, rows *sql.Rows, change func(name string) string) (err error) {
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
	if change == nil {
		change = defaultColumnNameToStructAttributeName
	}
	for index, column = range columns {
		columns[index] = change(column)
	}
	var field reflect.Value
	line := reflect.Indirect(reflect.New(rt))
	length := len(columns)
	scanner := make([]interface{}, length, length)
	cols := map[string]int{}
	for i := 0; i < line.NumField(); i++ {
		cols[line.Type().Field(i).Name] = i
	}
	var serial int
	var ok bool
	for index, column = range columns {
		serial, ok = cols[column]
		if !ok {
			err = fmt.Errorf("struct field `%s` does not match", column)
			return
		}
		field = line.Field(serial)
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

//
// Deprecated: Use Scanning instead.
//
// ScanAll any type: *[]AnyStruct | *[]*AnyStruct.
func ScanAll(any interface{}, rows *sql.Rows, change func(name string) string) (err error) {
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
			err = ScanAll2(any, rows, change)
		} else {
			err = errors.New("go-mysql: `any` slice element is neither a struct nor a struct pointer")
			return
		}
	} else if prt2.Kind() == reflect.Struct {
		// slice element is struct
		err = ScanAll1(any, rows, change)
	} else {
		err = errors.New("go-mysql: `any` slice element is neither a struct nor a struct pointer")
		return
	}
	return
}

//
// Deprecated: Use Scanning instead.
//
// ScanAll1 any type: *[]AnyStruct, it is recommended to call ScanAll.
func ScanAll1(any interface{}, rows *sql.Rows, change func(name string) string) (err error) {
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	var index int
	var column string
	if change == nil {
		change = defaultColumnNameToStructAttributeName
	}
	for index, column = range columns {
		columns[index] = change(column)
	}
	var line reflect.Value
	var value reflect.Value
	var field reflect.Value
	at := reflect.TypeOf(any)
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	lines := reflect.Indirect(reflect.New(at.Elem().Elem()))
	cols := map[string]int{}
	for i := 0; i < lines.NumField(); i++ {
		cols[lines.Type().Field(i).Name] = i
	}
	var serial int
	var ok bool
	for rows.Next() {
		line = reflect.New(at.Elem().Elem())
		value = reflect.Indirect(line)
		for index, column = range columns {
			serial, ok = cols[column]
			if !ok {
				err = fmt.Errorf("struct field `%s` does not match", column)
				return
			}
			field = value.Field(serial)
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

//
// Deprecated: Use Scanning instead.
//
// ScanAll2 any type: *[]*AnyStruct, it is recommended to call ScanAll.
func ScanAll2(any interface{}, rows *sql.Rows, change func(name string) string) (err error) {
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	var index int
	var column string
	if change == nil {
		change = defaultColumnNameToStructAttributeName
	}
	for index, column = range columns {
		columns[index] = change(column)
	}
	var line reflect.Value
	var value reflect.Value
	var field reflect.Value
	at := reflect.TypeOf(any)
	slices := reflect.ValueOf(any).Elem()
	length := len(columns)
	scanner := make([]interface{}, length, length)
	lines := reflect.Indirect(reflect.New(at.Elem().Elem().Elem()))
	cols := map[string]int{}
	for i := 0; i < lines.NumField(); i++ {
		cols[lines.Type().Field(i).Name] = i
	}
	var serial int
	var ok bool
	for rows.Next() {
		line = reflect.New(at.Elem().Elem().Elem())
		value = reflect.Indirect(line)
		for index, column = range columns {
			serial, ok = cols[column]
			if !ok {
				err = fmt.Errorf("struct field `%s` does not match", column)
				return
			}
			field = value.Field(serial)
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

// Scanning scan one or more rows.
func Scanning(any interface{}, rows *sql.Rows, change func(name string) string) (err error) {
	tp1 := reflect.TypeOf(any)
	if tp1.Kind() != reflect.Ptr {
		err = errors.New("go-mysql: receive variable is not a pointer")
		return
	}
	tp2 := tp1.Elem()
	err = fmt.Errorf("go-mysql: unsupported receive variable type *%s", tp2.Name())
	switch tp2.Kind() {
	// scan one row
	case reflect.Struct:
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
		if change == nil {
			change = defaultColumnNameToStructAttributeName
		}
		for index, column = range columns {
			columns[index] = change(column)
		}
		var field reflect.Value
		line := reflect.Indirect(reflect.New(tp2))
		length := len(columns)
		scanner := make([]interface{}, length, length)
		cols := map[string]int{}
		for i := 0; i < line.NumField(); i++ {
			cols[line.Type().Field(i).Name] = i
		}
		var serial int
		var ok bool
		for index, column = range columns {
			serial, ok = cols[column]
			if !ok {
				err = fmt.Errorf("struct field `%s` does not match", column)
				return
			}
			field = line.Field(serial)
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
	// scan more rows
	case reflect.Slice:
		tp3 := tp2.Elem()
		switch tp3.Kind() {
		case reflect.Ptr:
			if tp3.Elem().Kind() == reflect.Struct {
				var columns []string
				columns, err = rows.Columns()
				if err != nil {
					return
				}
				var index int
				var column string
				if change == nil {
					change = defaultColumnNameToStructAttributeName
				}
				for index, column = range columns {
					columns[index] = change(column)
				}
				var line reflect.Value
				var value reflect.Value
				var field reflect.Value
				slices := reflect.ValueOf(any).Elem()
				length := len(columns)
				scanner := make([]interface{}, length, length)
				lines := reflect.Indirect(reflect.New(tp1.Elem().Elem().Elem()))
				cols := map[string]int{}
				for i := 0; i < lines.NumField(); i++ {
					cols[lines.Type().Field(i).Name] = i
				}
				var serial int
				var ok bool
				for rows.Next() {
					line = reflect.New(tp1.Elem().Elem().Elem())
					value = reflect.Indirect(line)
					for index, column = range columns {
						serial, ok = cols[column]
						if !ok {
							err = fmt.Errorf("struct field `%s` does not match", column)
							return
						}
						field = value.Field(serial)
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
			}
		case reflect.Struct:
			var columns []string
			columns, err = rows.Columns()
			if err != nil {
				return
			}
			var index int
			var column string
			if change == nil {
				change = defaultColumnNameToStructAttributeName
			}
			for index, column = range columns {
				columns[index] = change(column)
			}
			var line reflect.Value
			var value reflect.Value
			var field reflect.Value
			slices := reflect.ValueOf(any).Elem()
			length := len(columns)
			scanner := make([]interface{}, length, length)
			lines := reflect.Indirect(reflect.New(tp1.Elem().Elem()))
			cols := map[string]int{}
			for i := 0; i < lines.NumField(); i++ {
				cols[lines.Type().Field(i).Name] = i
			}
			var serial int
			var ok bool
			for rows.Next() {
				line = reflect.New(tp1.Elem().Elem())
				value = reflect.Indirect(line)
				for index, column = range columns {
					serial, ok = cols[column]
					if !ok {
						err = fmt.Errorf("struct field `%s` does not match", column)
						return
					}
					field = value.Field(serial)
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
		default:
		}
	default:
	}
	return
}
