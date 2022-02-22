package gomysql

import (
	"database/sql"
	"encoding/json"
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

// Open connect to mysql service, auto set database connect
// dn: driver name, dsn: data source name
// username:password@tcp(host:port)/test?charset=utf8mb4&collation=utf8mb4_unicode_ci
func Open(dn string, dsn string) (err error) {
	db, err = sql.Open(dn, dsn)
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

// Db2 database curd object
func Db2() *Hat {
	return &Hat{
		db: db,
	}
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

// JsonTransfer by json marshal and unmarshal transfer data from source to result
func JsonTransfer(source interface{}, result interface{}) error {
	bts, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return json.Unmarshal(bts, result)
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

// Count sql count rows
func Count(prepare string, args ...interface{}) (int64, error) {
	return Db2().Count(prepare, args...)
}

// SumInt sql sum int
func SumInt(prepare string, args ...interface{}) (int64, error) {
	return Db2().SumInt(prepare, args...)
}

// SumFloat sql sum float
func SumFloat(prepare string, args ...interface{}) (float64, error) {
	return Db2().SumFloat(prepare, args...)
}

// Exists sql data exists
func Exists(prepare string, args ...interface{}) (bool, error) {
	return Db2().Exists(prepare, args...)
}

// Fetch query sql, fetch query result
func Fetch(fetch interface{}, prepare string, args ...interface{}) (err error) {
	return Db2().Prepare(prepare).Args(args...).Fetch(fetch)
}

// GetOneBts get first of query rows
func GetOneBts(prepare string, args ...interface{}) (map[string][]byte, error) {
	return Db2().Prepare(prepare).Args(args...).GetOneBts()
}

// GetAllBts get all of query rows
func GetAllBts(prepare string, args ...interface{}) ([]map[string][]byte, error) {
	return Db2().Prepare(prepare).Args(args...).GetAllBts()
}

// GetOneStr get first of query rows
func GetOneStr(prepare string, args ...interface{}) (map[string]*string, error) {
	return Db2().Prepare(prepare).Args(args...).GetOneStr()
}

// GetAllStr get all of query rows
func GetAllStr(prepare string, args ...interface{}) ([]map[string]*string, error) {
	return Db2().Prepare(prepare).Args(args...).GetAllStr()
}

// GetOneAny get first of query rows
func GetOneAny(prepare string, args ...interface{}) (map[string]interface{}, error) {
	return Db2().Prepare(prepare).Args(args...).GetOneAny()
}

// GetAllAny get all of query rows
func GetAllAny(prepare string, args ...interface{}) ([]map[string]interface{}, error) {
	return Db2().Prepare(prepare).Args(args...).GetAllAny()
}

// Hat mysql database sql statement execute object
type Hat struct {
	db      *sql.DB                          // database connection object
	tx      *sql.Tx                          // database transaction object
	prepare string                           // sql statement to be executed
	args    []interface{}                    // executed sql parameters
	scan    func(rows *sql.Rows) (err error) // scan query results
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

// PrepareArgs get prepared sql statement and parameter list of prepared sql statement
func (s *Hat) PrepareArgs() (string, []interface{}) {
	return s.prepare, s.args
}

// stmt execute the prepared sql statement, if the transaction has already started, use the transaction to execute the prepared sql statement first
func (s *Hat) stmt() (*sql.Stmt, error) {
	if s.tx != nil {
		return s.tx.Prepare(s.prepare)
	} else {
		return s.db.Prepare(s.prepare)
	}
}

// stmtQuery stmt query
func (s *Hat) stmtQuery() (*sql.Rows, error) {
	stmt, err := s.stmt()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Query(s.args...)
}

// stmtExec stmt exec
func (s *Hat) stmtExec() (sql.Result, error) {
	stmt, err := s.stmt()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Exec(s.args...)
}

// Query execute query sql
func (s *Hat) Query() error {
	rows, err := s.stmtQuery()
	if err != nil {
		return err
	}
	defer rows.Close()
	return s.scan(rows)
}

// Execute execute non-query sql
func (s *Hat) Execute() (int64, error) {
	result, err := s.stmtExec()
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Create execute the insert sql statement and get the self-increasing primary key value
func (s *Hat) Create() (int64, error) {
	result, err := s.stmtExec()
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// Count sql count rows
func (s *Hat) Count(prepare string, args ...interface{}) (count int64, err error) {
	err = s.Scan(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			err = rows.Scan(&count)
		}
		return
	}).Prepare(prepare).Args(args...).Query()
	return
}

// SumInt sql sum int
func (s *Hat) SumInt(prepare string, args ...interface{}) (sum int64, err error) {
	err = s.Scan(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var tmp *int64
			err = rows.Scan(&tmp)
			if err != nil {
				return
			}
			if tmp != nil {
				sum = *tmp
			}
		}
		return
	}).Prepare(prepare).Args(args...).Query()
	return
}

// SumFloat sql sum float
func (s *Hat) SumFloat(prepare string, args ...interface{}) (sum float64, err error) {
	err = s.Scan(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			var tmp *float64
			err = rows.Scan(&tmp)
			if err != nil {
				return
			}
			if tmp != nil {
				sum = *tmp
			}
		}
		return
	}).Prepare(prepare).Args(args...).Query()
	return
}

// Exists sql data exists
func (s *Hat) Exists(prepare string, args ...interface{}) (exists bool, err error) {
	err = s.Scan(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			exists = true
		}
		return
	}).Prepare(prepare).Args(args...).Query()
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

// Fetch scan one or more rows to interface{}
func (s *Hat) Fetch(fetch interface{}) (err error) {
	if fetch == nil {
		err = errors.New("receive object value is nil")
		return
	}
	tp := reflect.TypeOf(fetch)
	if tp.Kind() != reflect.Ptr {
		err = errors.New("receive object is not a pointer")
		return
	}
	tp = tp.Elem()
	var rows *sql.Rows
	switch tp.Kind() {
	case reflect.Struct:
		rows, err = s.stmtQuery()
		if err != nil {
			return
		}
		defer rows.Close()
		var first map[string]interface{}
		first, err = s.getOneAny(rows)
		if err != nil {
			return
		}
		err = JsonTransfer(first, fetch)
	case reflect.Slice:
		rows, err = s.stmtQuery()
		if err != nil {
			return
		}
		defer rows.Close()
		var all []map[string]interface{}
		all, err = s.getAllAny(rows)
		if err != nil {
			return
		}
		err = JsonTransfer(all, fetch)
	default:
		err = errors.New("receiving object is neither a struct pointer nor a slice pointer")
		return
	}
	return
}

// GetOneBts scan one to map[string][]byte the query result is empty and return => nil, nil
func (s *Hat) GetOneBts() (first map[string][]byte, err error) {
	var rows *sql.Rows
	rows, err = s.stmtQuery()
	if err != nil {
		return
	}
	defer rows.Close()
	first, err = s.getOneBts(rows)
	return
}

// GetAllBts scan all to []map[string][]byte the query result is empty and return => []map[string][]byte{}, nil
func (s *Hat) GetAllBts() (all []map[string][]byte, err error) {
	var rows *sql.Rows
	rows, err = s.stmtQuery()
	if err != nil {
		return
	}
	defer rows.Close()
	all, err = s.getAllBts(rows)
	return
}

// getOneBts the query result is empty and return => nil, nil
func (s *Hat) getOneBts(rows *sql.Rows) (first map[string][]byte, err error) {
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
	first = map[string][]byte{}
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
		first[columns[key]] = val
	}
	return
}

// getAllBts the query result is empty and return => []map[string][]byte{}, nil
func (s *Hat) getAllBts(rows *sql.Rows) (all []map[string][]byte, err error) {
	var length int
	var columns []string
	var tmp [][]byte
	var scanner []interface{}
	var line map[string][]byte
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	length = len(columns)
	all = []map[string][]byte{}
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
		line = map[string][]byte{}
		for key, val := range tmp {
			line[columns[key]] = val
		}
		all = append(all, line)
	}
	return
}

// GetOneStr scan one to map[string]*string the query result is empty and return => nil, nil
func (s *Hat) GetOneStr() (first map[string]*string, err error) {
	var rows *sql.Rows
	rows, err = s.stmtQuery()
	if err != nil {
		return
	}
	defer rows.Close()
	first, err = s.getOneStr(rows)
	return
}

// GetAllStr scan all to []map[string]*string the query result is empty and return => []map[string]*string{}, nil
func (s *Hat) GetAllStr() (all []map[string]*string, err error) {
	var rows *sql.Rows
	rows, err = s.stmtQuery()
	if err != nil {
		return
	}
	defer rows.Close()
	all, err = s.getAllStr(rows)
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
	var rows *sql.Rows
	rows, err = s.stmtQuery()
	if err != nil {
		return
	}
	defer rows.Close()
	first, err = s.getOneAny(rows)
	return
}

// GetAllAny scan all to []map[string]interface{} the query result is empty and return => []map[string]interface{}{}, nil
func (s *Hat) GetAllAny() (all []map[string]interface{}, err error) {
	var rows *sql.Rows
	rows, err = s.stmtQuery()
	if err != nil {
		return
	}
	defer rows.Close()
	all, err = s.getAllAny(rows)
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
