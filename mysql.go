package gomysql

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

func Db2() *Execs {
	return &Execs{
		db: db,
	}
}

func Query(anonymous func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return Db2().RightQuery(anonymous, prepare, args...)
}

func Execute(prepare string, args ...interface{}) (int64, error) {
	return Db2().RightExecute(prepare, args...)
}

func Transaction(times int, anonymous func(execs *Execs) (err error)) error {
	return Db2().Transaction(times, anonymous)
}

func Create(prepare string, args ...interface{}) (int64, error) {
	return Db2().RightCreate(prepare, args...)
}

func Fetch(any interface{}, prepare string, args ...interface{}) (err error) {
	err = Db2().RightFetch(any, prepare, args...)
	return
}

// Execs mysql database sql statement execute object
type Execs struct {
	db      *sql.DB                          // database connection object
	tx      *sql.Tx                          // database transaction object
	prepare string                           // sql statement to be executed
	args    []interface{}                    // executed sql parameters
	scan    func(rows *sql.Rows) (err error) // scan query results
	change  func(name string) string         // when driver scan column name to struct name
}

func (s *Execs) Begin() (err error) {
	s.tx, err = s.db.Begin()
	return
}

func (s *Execs) Rollback() (err error) {
	if s.tx == nil {
		err = ErrTransactionNotOpened
		return
	}
	err = s.tx.Rollback()
	s.tx = nil
	return
}

func (s *Execs) Commit() (err error) {
	if s.tx == nil {
		err = ErrTransactionNotOpened
		return
	}
	err = s.tx.Commit()
	s.tx = nil
	return
}

func (s *Execs) Scan(anonymous func(rows *sql.Rows) (err error)) *Execs {
	s.scan = anonymous
	return s
}

func (s *Execs) Prepare(prepare string) *Execs {
	s.prepare = prepare
	return s
}

func (s *Execs) Args(args ...interface{}) *Execs {
	s.args = args
	return s
}

func (s *Execs) Stmt() (stmt *sql.Stmt, err error) {
	if s.tx != nil {
		stmt, err = s.tx.Prepare(s.prepare)
	} else {
		stmt, err = s.db.Prepare(s.prepare)
	}
	return
}

func (s *Execs) PrepareArgs() (prepare string, args []interface{}) {
	prepare, args = s.prepare, s.args
	return
}

func (s *Execs) Query() (err error) {
	var stmt *sql.Stmt
	stmt, err = s.Stmt()
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

func (s *Execs) Execute() (rowsAffected int64, err error) {
	var stmt *sql.Stmt
	stmt, err = s.Stmt()
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

func (s *Execs) Create() (lastId int64, err error) {
	var stmt *sql.Stmt
	stmt, err = s.Stmt()
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

func (s *Execs) RightQuery(anonymous func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	err = s.Scan(anonymous).Prepare(prepare).Args(args...).Query()
	return
}

func (s *Execs) RightExecute(prepare string, args ...interface{}) (int64, error) {
	return s.Prepare(prepare).Args(args...).Execute()
}

func (s *Execs) RightCreate(prepare string, args ...interface{}) (int64, error) {
	return s.Prepare(prepare).Args(args...).Create()
}

// Transaction closure execute transaction, automatic rollback on error
func (s *Execs) Transaction(times int, anonymous func(execs *Execs) (err error)) (err error) {
	if times <= 0 {
		err = fmt.Errorf("mysql: the number of transactions executed by the database has been used up")
		return
	}
	for i := 0; i < times; i++ {
		err = s.Begin()
		if err != nil {
			continue
		}
		err = anonymous(s)
		if err != nil {
			_ = s.Rollback()
			continue
		}
		_ = s.Commit()
		break
	}
	return
}

func (s *Execs) Change(change func(name string) string) {
	s.change = change
}

// Fetch scan one or more rows to interface{}
func (s *Execs) Fetch(any interface{}) (err error) {
	var stmt *sql.Stmt
	stmt, err = s.Stmt()
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
	err = Scanning(any, rows, s.change)
	if err != nil {
		return
	}
	return
}

func (s *Execs) RightFetch(any interface{}, prepare string, args ...interface{}) (err error) {
	err = s.Prepare(prepare).Args(args...).Fetch(any)
	return
}
