package gomysql

import (
	"database/sql"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	Backtick = "`"
)

// db database object
var db *sql.DB

// ErrTransactionNotOpened transaction not opened
var ErrTransactionNotOpened = errors.New("go-mysql: please open the transaction first")

// Open connect to mysql service, auto set database connect; dsn: runner:112233@tcp(127.0.0.1:3306)/running?charset=utf8mb4&collation=utf8mb4_unicode_ci
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

func Db0(database *sql.DB) {
	db = database
}

func Db1() *sql.DB {
	return db
}

func Db2() *Hat {
	return &Hat{
		db: db,
	}
}

func Query(anonymous func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return Db2().Scan(anonymous).Prepare(prepare).Args(args...).Query()
}

func Execute(prepare string, args ...interface{}) (int64, error) {
	return Db2().Prepare(prepare).Args(args...).Execute()
}

func Transaction(anonymous func(x *Hat) (err error)) error {
	return Db2().Transaction(anonymous)
}

func Create(prepare string, args ...interface{}) (int64, error) {
	return Db2().Prepare(prepare).Args(args...).Create()
}

func Fetch(any interface{}, prepare string, args ...interface{}) (err error) {
	return Db2().Prepare(prepare).Args(args...).Fetch(any)
}

// Hat mysql database sql statement execute object
type Hat struct {
	db               *sql.DB                          // database connection object
	tx               *sql.Tx                          // database transaction object
	prepare          string                           // sql statement to be executed
	args             []interface{}                    // executed sql parameters
	scan             func(rows *sql.Rows) (err error) // scan query results
	column2attribute func(name string) string         // when driver scan column, table column name to struct attribute name.
	attribute2column func(name string) string         // when driver insert table, struct attribute name to table column name.
}

func (s *Hat) Begin() (err error) {
	s.tx, err = s.db.Begin()
	return
}

func (s *Hat) Rollback() (err error) {
	if s.tx == nil {
		err = ErrTransactionNotOpened
		return
	}
	err = s.tx.Rollback()
	s.tx = nil
	return
}

func (s *Hat) Commit() (err error) {
	if s.tx == nil {
		err = ErrTransactionNotOpened
		return
	}
	err = s.tx.Commit()
	s.tx = nil
	return
}

func (s *Hat) Scan(anonymous func(rows *sql.Rows) (err error)) *Hat {
	s.scan = anonymous
	return s
}

func (s *Hat) Prepare(prepare string) *Hat {
	s.prepare = prepare
	return s
}

func (s *Hat) Args(args ...interface{}) *Hat {
	s.args = args
	return s
}

func (s *Hat) stmt() (stmt *sql.Stmt, err error) {
	if s.tx != nil {
		stmt, err = s.tx.Prepare(s.prepare)
	} else {
		stmt, err = s.db.Prepare(s.prepare)
	}
	return
}

func (s *Hat) PrepareArgs() (prepare string, args []interface{}) {
	prepare, args = s.prepare, s.args
	return
}

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

func (s *Hat) Create() (lastId int64, err error) {
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
	lastId, err = result.LastInsertId()
	return
}

// Transaction closure execute transaction, automatic rollback on error
func (s *Hat) Transaction(anonymous func(x *Hat) (err error)) (err error) {
	err = s.Begin()
	if err != nil {
		return
	}
	err = anonymous(s)
	if err != nil {
		_ = s.Rollback()
		return
	}
	_ = s.Commit()
	return
}

func (s *Hat) ColumnToAttribute(column2attribute func(name string) string) {
	s.column2attribute = column2attribute
}

func (s *Hat) AttributeToColumn(attribute2column func(name string) string) {
	s.attribute2column = attribute2column
}

// Fetch scan one or more rows to interface{}
func (s *Hat) Fetch(any interface{}) (err error) {
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
	err = Scanning(any, rows, s.column2attribute)
	if err != nil {
		return
	}
	return
}
