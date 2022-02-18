package gomysql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	Backtick = "`" // backtick
)

// db database connect object
var db *sql.DB

// Open connect to mysql service, auto set database connect; dsn: username:password@tcp(host:port)/test?charset=utf8mb4&collation=utf8mb4_unicode_ci
func Open(dsn string) (err error) {
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(512)
	db.SetMaxIdleConns(128)
	return
}

// Db0 set database connect object
func Db0(database *sql.DB) {
	db = database
}

// Db1 get database connect object
func Db1() *sql.DB {
	return db
}

// defaultName0 mysql name to go name
var defaultName0 = func(name string) string {
	return UnderlineToPascal(strings.ToLower(name))
}

// defaultName1 go name to mysql name
var defaultName1 = func(name string) string {
	return PascalToUnderline(name)
}

// Db2 database curd object
func Db2() *Hat {
	return &Hat{
		db:    db,
		name0: defaultName0,
		name1: defaultName1,
	}
}

// Identifier MySql identifier
func Identifier(s string) string {
	if strings.Contains(s, "(") {
		// there is an identifier for a function call, do nothing
		return s
	}
	s = strings.ReplaceAll(s, Backtick, "")
	s = strings.ReplaceAll(s, ".", fmt.Sprintf("%s.%s", Backtick, Backtick))
	s = fmt.Sprintf("%s%s%s", Backtick, s, Backtick)
	return s
}

// PascalToUnderline XxxYyy to xxx_yyy
func PascalToUnderline(s string) string {
	var tmp []byte
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			tmp = append(tmp, '_')
		}
		if d != '_' {
			j = true
		}
		tmp = append(tmp, d)
	}
	return strings.ToLower(string(tmp[:]))
}

// UnderlineToPascal xxx_yyy to XxxYyy
func UnderlineToPascal(s string) string {
	var tmp []byte
	bytes := []byte(s)
	length := len(bytes)
	nextLetterNeedToUpper := true
	for i := 0; i < length; i++ {
		if bytes[i] == '_' {
			nextLetterNeedToUpper = true
			continue
		}
		if nextLetterNeedToUpper && bytes[i] >= 'a' && bytes[i] <= 'z' {
			tmp = append(tmp, bytes[i]-32)
		} else {
			tmp = append(tmp, bytes[i])
		}
		nextLetterNeedToUpper = false
	}
	return string(tmp[:])
}

// SetDefaultName0 mysql name to go name
func SetDefaultName0(name0 func(name string) string) {
	defaultName0 = name0
}

// SetDefaultName1 go name to mysql name
func SetDefaultName1(name1 func(name string) string) {
	defaultName1 = name1
}

// Query execute query sql
func Query(scan func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return Db2().Scan(scan).Prepare(prepare).Args(args...).Query()
}

// Execute execute non-query sql
func Execute(prepare string, args ...interface{}) (int64, error) {
	return Db2().Prepare(prepare).Args(args...).Execute()
}

// Transaction transaction execution, automatic rollback on error
func Transaction(closure func(hat *Hat) (err error)) error {
	return Db2().Transaction(closure)
}

// Create execute insert sql
func Create(prepare string, args ...interface{}) (int64, error) {
	return Db2().Prepare(prepare).Args(args...).Create()
}

// Fetch query sql, automatically match fields according to naming rules
func Fetch(fetch interface{}, prepare string, args ...interface{}) (err error) {
	return Db2().Prepare(prepare).Args(args...).Fetch(fetch)
}

// Hat mysql database sql statement execute object
type Hat struct {
	db      *sql.DB                          // database connection object
	tx      *sql.Tx                          // database transaction object
	prepare string                           // sql statement to be executed
	args    []interface{}                    // executed sql parameters
	scan    func(rows *sql.Rows) (err error) // scan query results
	name0   func(name string) string         // mysql name to go name
	name1   func(name string) string         // go name to mysql name
}

// Begin start a transaction
func (s *Hat) Begin() (err error) {
	if s.tx != nil {
		err = errors.New("please commit or rollback the opened transaction")
		return
	}
	s.tx, err = s.db.Begin()
	return
}

// Rollback transaction rollback
func (s *Hat) Rollback() (err error) {
	if s.tx != nil {
		err = s.tx.Rollback()
		s.tx = nil
	}
	return
}

// Commit transaction commit
func (s *Hat) Commit() (err error) {
	if s.tx != nil {
		err = s.tx.Commit()
		s.tx = nil
	}
	return
}

// Scan set scan query result (anonymous function)
func (s *Hat) Scan(scan func(rows *sql.Rows) (err error)) *Hat {
	s.scan = scan
	return s
}

// Prepare set prepared sql statement
func (s *Hat) Prepare(prepare string) *Hat {
	s.prepare = prepare
	return s
}

// Args set the parameter list of the prepared sql statement
func (s *Hat) Args(args ...interface{}) *Hat {
	s.args = args
	return s
}

// stmt execute the prepared sql statement, if the transaction has already started, use the transaction to execute the prepared sql statement first
func (s *Hat) stmt() (stmt *sql.Stmt, err error) {
	if s.tx != nil {
		stmt, err = s.tx.Prepare(s.prepare)
	} else {
		stmt, err = s.db.Prepare(s.prepare)
	}
	return
}

// PrepareArgs get prepared sql statement and parameter list of prepared sql statement
func (s *Hat) PrepareArgs() (prepare string, args []interface{}) {
	prepare, args = s.prepare, s.args
	return
}

// Query execute query sql
func (s *Hat) Query() (err error) {
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(s.args...)
	if err != nil {
		return
	}
	defer rows.Close()
	err = s.scan(rows)
	return
}

// Execute execute non-query sql
func (s *Hat) Execute() (rowsAffected int64, err error) {
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var result sql.Result
	result, err = stmt.Exec(s.args...)
	if err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}

// Create execute the insert sql statement and get the self-increasing primary key value
func (s *Hat) Create() (lastInsertId int64, err error) {
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var result sql.Result
	result, err = stmt.Exec(s.args...)
	if err != nil {
		return
	}
	lastInsertId, err = result.LastInsertId()
	return
}

// Transaction closure execute transaction, automatic rollback on error
func (s *Hat) Transaction(closure func(hat *Hat) (err error)) (err error) {
	err = s.Begin()
	if err != nil {
		return
	}
	err = closure(s)
	if err != nil {
		_ = s.Rollback()
		return
	}
	_ = s.Commit()
	return
}

// Name0 mysql name to go name
func (s *Hat) Name0(name0 func(name string) string) {
	s.name0 = name0
}

// Name1 go name to mysql name
func (s *Hat) Name1(name1 func(name string) string) {
	s.name1 = name1
}

// scanning scan one or more rows.
func (s *Hat) scanning(any interface{}, rows *sql.Rows, change func(name string) string) (err error) {
	tp1 := reflect.TypeOf(any)
	if tp1.Kind() != reflect.Ptr {
		err = errors.New("sql: receive variable is not a pointer")
		return
	}
	tp2 := tp1.Elem()
	switch tp2.Kind() {
	case reflect.Struct:
		// any: *AnyStruct
		if !rows.Next() {
			// 查询结果集为空(没有查询到匹配的行)
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
			change = defaultName0
		}
		for index, column = range columns {
			columns[index] = change(column)
		}
		var field reflect.Value
		line := reflect.Indirect(reflect.New(tp2))
		length := len(columns)
		scanner := make([]interface{}, length)
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
	case reflect.Slice:
		tp3 := tp2.Elem()
		switch tp3.Kind() {
		// any: *[]*AnyStruct
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
					change = defaultName0
				}
				for index, column = range columns {
					columns[index] = change(column)
				}
				var line reflect.Value
				var value reflect.Value
				var field reflect.Value
				slices := reflect.ValueOf(any).Elem()
				length := len(columns)
				scanner := make([]interface{}, length)
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
					// 查询结果集为空(没有查询到匹配的行)
					reflect.ValueOf(any).Elem().Set(reflect.MakeSlice(tp2, 0, 0))
					return
				}
				// 查询到结果, 通过反射设置查询结果值
				reflect.ValueOf(any).Elem().Set(slices)
			}
		// any: *[]AnyStruct
		case reflect.Struct:
			var columns []string
			columns, err = rows.Columns()
			if err != nil {
				return
			}
			var index int
			var column string
			if change == nil {
				change = defaultName0
			}
			for index, column = range columns {
				columns[index] = change(column)
			}
			var line reflect.Value
			var value reflect.Value
			var field reflect.Value
			slices := reflect.ValueOf(any).Elem()
			length := len(columns)
			scanner := make([]interface{}, length)
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
				// 查询结果集为空(没有查询到匹配的行)
				reflect.ValueOf(any).Elem().Set(reflect.MakeSlice(tp2, 0, 0))
				return
			}
			// 查询到结果, 通过反射设置查询结果值
			reflect.ValueOf(any).Elem().Set(slices)
		default:
		}
	default:
		err = fmt.Errorf("sql: unsupported receive variable type *%s", tp2.Name())
	}
	return
}

// Fetch scan one or more rows to interface{}
func (s *Hat) Fetch(fetch interface{}) (err error) {
	if fetch == nil {
		err = errors.New("receive object value is nil")
		return
	}
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(s.args...)
	if err != nil {
		return
	}
	defer rows.Close()
	err = s.scanning(fetch, rows, s.name0)
	if err != nil {
		return
	}
	return
}

// GetOneStr scan one to map[string]*string the query result is empty and return => nil, nil
func (s *Hat) GetOneStr() (first map[string]*string, err error) {
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(s.args...)
	if err != nil {
		return
	}
	defer rows.Close()
	first, err = s.getOneStr(rows)
	if err != nil {
		return
	}
	return
}

// GetAllStr scan all to []map[string]*string the query result is empty and return => []map[string]*string{}, nil
func (s *Hat) GetAllStr() (all []map[string]*string, err error) {
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(s.args...)
	if err != nil {
		return
	}
	defer rows.Close()
	all, err = s.getAllStr(rows)
	if err != nil {
		return
	}
	return
}

// getOneStr the query result is empty and return => nil, nil
func (s *Hat) getOneStr(rows *sql.Rows) (first map[string]*string, err error) {
	if !rows.Next() {
		return
	}
	var length int
	var columns []string
	var scanner []interface{}
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	length = len(columns)
	first = map[string]*string{}
	tmp := make([][]byte, length)
	scanner = make([]interface{}, length)
	for i := range tmp {
		scanner[i] = &tmp[i]
	}
	err = rows.Scan(scanner...)
	if err != nil {
		return
	}
	for key, val := range tmp {
		if val == nil {
			first[columns[key]] = nil
		} else {
			str := string(val)
			first[columns[key]] = &str
		}
	}
	return
}

// getAllStr the query result is empty and return => []map[string]*string{}, nil
func (s *Hat) getAllStr(rows *sql.Rows) (all []map[string]*string, err error) {
	var length int
	var columns []string
	var tmp [][]byte
	var scanner []interface{}
	var line map[string]*string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	length = len(columns)
	all = []map[string]*string{}
	for rows.Next() {
		tmp = make([][]byte, length)
		scanner = make([]interface{}, length)
		for i := range tmp {
			scanner[i] = &tmp[i]
		}
		err = rows.Scan(scanner...)
		if err != nil {
			return
		}
		line = map[string]*string{}
		for key, val := range tmp {
			if val == nil {
				line[columns[key]] = nil
			} else {
				str := string(val)
				line[columns[key]] = &str
			}
		}
		all = append(all, line)
	}
	return
}

// GetOneAny scan one to map[string]interface{} the query result is empty and return => nil, nil
func (s *Hat) GetOneAny() (first map[string]interface{}, err error) {
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(s.args...)
	if err != nil {
		return
	}
	defer rows.Close()
	first, err = s.getOneAny(rows)
	if err != nil {
		return
	}
	return
}

// GetAllAny scan all to []map[string]interface{} the query result is empty and return => []map[string]interface{}{}, nil
func (s *Hat) GetAllAny() (all []map[string]interface{}, err error) {
	var stmt *sql.Stmt
	stmt, err = s.stmt()
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(s.args...)
	if err != nil {
		return
	}
	defer rows.Close()
	all, err = s.getAllAny(rows)
	if err != nil {
		return
	}
	return
}

func DataTypeMysqlToGo(sqlColumnType *sql.ColumnType, sqlValue interface{}) (result interface{}, err error) {
	result = sqlValue
	if sqlValue == nil {
		return
	}
	dtn := sqlColumnType.DatabaseTypeName()
	if bts, ok := sqlValue.([]byte); ok {
		switch dtn {
		case "DECIMAL", "DOUBLE", "FLOAT":
			result, err = strconv.ParseFloat(string(bts), 64)
			return
		default:
			result = string(bts)
		}
		return
	}
	if bts, ok := sqlValue.(*[]byte); ok {
		switch dtn {
		case "DECIMAL", "DOUBLE", "FLOAT":
			result, err = strconv.ParseFloat(string(*bts), 64)
			return
		default:
			result = string(*bts)
		}
		return
	}
	return
}

// getOneAny the query result is empty and return => nil, nil
func (s *Hat) getOneAny(rows *sql.Rows) (first map[string]interface{}, err error) {
	if !rows.Next() {
		return
	}
	var length int
	var columnTypes []*sql.ColumnType
	var scanner []interface{}
	columnTypes, err = rows.ColumnTypes()
	if err != nil {
		return
	}
	length = len(columnTypes)
	first = map[string]interface{}{}
	tmp := make([]interface{}, length)
	scanner = make([]interface{}, length)
	for i := range tmp {
		scanner[i] = &tmp[i]
	}
	err = rows.Scan(scanner...)
	if err != nil {
		return
	}
	for key, val := range tmp {
		first[columnTypes[key].Name()], err = DataTypeMysqlToGo(columnTypes[key], val)
		if err != nil {
			return
		}
	}
	return
}

// getAllAny the query result is empty and return => []map[string]interface{}{}, nil
func (s *Hat) getAllAny(rows *sql.Rows) (all []map[string]interface{}, err error) {
	var length int
	var columnTypes []*sql.ColumnType
	var tmp []interface{}
	var scanner []interface{}
	var line map[string]interface{}
	columnTypes, err = rows.ColumnTypes()
	if err != nil {
		return
	}
	length = len(columnTypes)
	all = []map[string]interface{}{}
	for rows.Next() {
		tmp = make([]interface{}, length)
		scanner = make([]interface{}, length)
		for i := range tmp {
			scanner[i] = &tmp[i]
		}
		err = rows.Scan(scanner...)
		if err != nil {
			return
		}
		line = map[string]interface{}{}
		for key, val := range tmp {
			line[columnTypes[key].Name()], err = DataTypeMysqlToGo(columnTypes[key], val)
		}
		all = append(all, line)
	}
	return
}
