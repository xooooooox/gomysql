package gomysql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// modify convert map data updated as needed into field slices and field parameters
func modify(update map[string]interface{}) (columns []string, args []interface{}) {
	length := len(update)
	if length == 0 {
		return
	}
	columns = make([]string, length)
	args = make([]interface{}, length)
	i := 0
	for key := range update {
		columns[i] = key
		i++
	}
	sort.Strings(columns)
	for key, val := range columns {
		args[key] = update[val]
	}
	return
}

// ModifyPrepareArgs convert the updated map into sql update script and corresponding parameters
func ModifyPrepareArgs(update map[string]interface{}) (prepare string, args []interface{}) {
	var columns []string
	columns, args = modify(update)
	for key, val := range columns {
		columns[key] = fmt.Sprintf("%s = ?", Identifier(val))
	}
	prepare = strings.Join(columns, ", ")
	return
}

// Curd insert, update, delete, select
type Curd struct {
	hat   *Hat
	AddAt func() map[string]interface{}
	ModAt func() map[string]interface{}
	DelAt func() map[string]interface{}
}

func NewCurd(hat ...*Hat) (curd *Curd) {
	curd = &Curd{}
	length := len(hat)
	if length > 0 {
		curd.hat = hat[length-1]
	} else {
		curd.hat = Db2()
	}
	return
}

// Transaction closures execute transaction, err != nil auto rollback
func (s *Curd) Transaction(closure func(curd *Curd) (err error)) (err error) {
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

// Begin start a transaction
func (s *Curd) Begin() error {
	return s.hat.Begin()
}

// Rollback transaction rollback
func (s *Curd) Rollback() error {
	return s.hat.Rollback()
}

// Commit transaction commit
func (s *Curd) Commit() error {
	return s.hat.Commit()
}

// PrepareArgs get prepared sql statement and parameter list of prepared sql statement
func (s *Curd) PrepareArgs() (prepare string, args []interface{}) {
	prepare, args = s.hat.PrepareArgs()
	return
}

// JsonFirst fetch first one using json
func (s *Curd) JsonFirst(fetch interface{}, prepare string, args ...interface{}) (empty bool, err error) {
	empty, err = s.hat.Prepare(prepare).Args(args...).JsonFirst(fetch)
	return
}

// JsonAll fetch all using json
func (s *Curd) JsonAll(fetch interface{}, prepare string, args ...interface{}) error {
	return s.hat.Prepare(prepare).Args(args...).JsonAll(fetch)
}

// GetFirst get first one
func (s *Curd) GetFirst(prepare string, args ...interface{}) (map[string]interface{}, error) {
	return s.hat.Prepare(prepare).Args(args...).GetFirst()
}

// GetAll get all
func (s *Curd) GetAll(prepare string, args ...interface{}) ([]map[string]interface{}, error) {
	return s.hat.Prepare(prepare).Args(args...).GetAll()
}

// GetFirstByte get first one
func (s *Curd) GetFirstByte(prepare string, args ...interface{}) (map[string][]byte, error) {
	return s.hat.Prepare(prepare).Args(args...).GetFirstByte()
}

// GetAllByte get all
func (s *Curd) GetAllByte(prepare string, args ...interface{}) ([]map[string][]byte, error) {
	return s.hat.Prepare(prepare).Args(args...).GetAllByte()
}

// JsonTransfer data exchange by json, map[string]interface{} <=> *AnyStruct , []map[string]interface{} <=> *[]AnyStruct | *[]*AnyStruct
func (s *Curd) JsonTransfer(source interface{}, result interface{}) error {
	return JsonTransfer(source, result)
}

// Query execute any query sql
func (s *Curd) Query(scan func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return s.hat.Scan(scan).Prepare(prepare).Args(args...).Query()
}

// Execute execute any non-query sql
func (s *Curd) Execute(prepare string, args ...interface{}) (int64, error) {
	return s.hat.Prepare(prepare).Args(args...).Execute()
}

// Create execute an insert sql
func (s *Curd) Create(prepare string, args ...interface{}) (int64, error) {
	return s.hat.Prepare(prepare).Args(args...).Create()
}

// table cout table name
func (s *Curd) table(table interface{}) (name string) {
	if table == nil {
		return
	}
	ok := false
	name, ok = table.(string)
	if ok {
		return
	}
	tp := reflect.TypeOf(table)
	if tp.Kind() == reflect.Struct {
		name = PascalToUnderline(tp.Name())
		return
	}
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
		if tp.Kind() == reflect.Struct {
			name = PascalToUnderline(tp.Name())
			return
		}
	}
	return
}

// addAt append timestamp
func (s *Curd) addAt(msi map[string]interface{}, add func() map[string]interface{}) map[string]interface{} {
	if add == nil {
		return msi
	}
	apd := add()
	if apd == nil {
		return msi
	}
	if msi == nil {
		msi = map[string]interface{}{}
	}
	for k, v := range apd {
		if _, ok := msi[k]; !ok {
			msi[k] = v
		}
	}
	return msi
}

// Add insert a piece of data
func (s *Curd) Add(add interface{}, table ...interface{}) (id int64, err error) {
	if add == nil {
		err = errors.New("insert object is nil")
		return
	}
	tab := ""
	length := len(table)
	if length > 0 {
		tab = s.table(table[length-1])
	} else {
		tab = s.table(add)
	}
	if tab == "" {
		err = errors.New("please set table name first")
		return
	}
	obj := map[string]interface{}{}
	ok := false
	obj, ok = add.(map[string]interface{})
	if !ok {
		err = s.JsonTransfer(add, &obj)
		if err != nil {
			return
		}
	}
	if s.AddAt != nil {
		obj = s.addAt(obj, s.AddAt)
	}
	length = len(obj)
	columns := make([]string, length)
	values := make([]string, length)
	args := make([]interface{}, length)
	i := 0
	for key := range obj {
		columns[i] = key
		values[i] = "?"
		i++
	}
	// sort by field name
	sort.Strings(columns)
	for key, val := range columns {
		args[key] = obj[val]
	}
	prepare := fmt.Sprintf(
		"INSERT INTO %s ( %s ) VALUES ( %s );",
		Identifier(tab),
		fmt.Sprintf("%s%s%s", Backtick, strings.Join(columns, fmt.Sprintf("%s, %s", Backtick, Backtick)), Backtick),
		strings.Join(values, ", "),
	)
	id, err = s.Create(prepare, args...)
	return
}

// ideq id equal
func ideq() string {
	return "`id` = ?"
}

// Del delete using where
func (s *Curd) Del(table interface{}, where string, args ...interface{}) (int64, error) {
	tab := s.table(table)
	if tab == "" {
		return 0, errors.New("please set table name first")
	}
	if where == "" {
		return s.Execute(fmt.Sprintf("DELETE FROM %s;", Identifier(tab)))
	}
	return s.Execute(fmt.Sprintf("DELETE FROM %s WHERE ( %s );", Identifier(tab), where), args...)
}

// DelId delete using id
func (s *Curd) DelId(table interface{}, id interface{}) (int64, error) {
	return s.Del(table, ideq(), id)
}

// FakDel fake delete using where
func (s *Curd) FakDel(table interface{}, where string, args ...interface{}) (int64, error) {
	if s.DelAt == nil {
		return 0, errors.New("please set the pseudo delete handler first")
	}
	update := s.DelAt()
	length := len(update)
	if length == 0 {
		return 0, nil
	}
	tab := s.table(table)
	if tab == "" {
		return 0, errors.New("please set table name first")
	}
	key, val := ModifyPrepareArgs(update)
	prepare := ""
	if where == "" {
		prepare = fmt.Sprintf("UPDATE %s SET %s;", Identifier(tab), key)
	} else {
		prepare = fmt.Sprintf("UPDATE %s SET %s WHERE ( %s );", Identifier(tab), key, where)
		val = append(val, args...)
	}
	return s.Execute(prepare, val...)
}

// FakDelId fake delete using id
func (s *Curd) FakDelId(table interface{}, id interface{}) (int64, error) {
	return s.DelId(table, id)
}

// Mod modify using map[string]interface{}
func (s *Curd) Mod(update map[string]interface{}, table interface{}, where string, args ...interface{}) (int64, error) {
	tab := s.table(table)
	if tab == "" {
		return 0, errors.New("please set table name first")
	}
	if s.ModAt != nil {
		update = s.addAt(update, s.ModAt)
	}
	key, val := ModifyPrepareArgs(update)
	prepare := ""
	if where == "" {
		prepare = fmt.Sprintf("UPDATE %s SET %s;", Identifier(tab), key)
	} else {
		prepare = fmt.Sprintf("UPDATE %s SET %s WHERE ( %s );", Identifier(tab), key, where)
		val = append(val, args...)
	}
	return s.Execute(prepare, val...)
}

// ModId modify using map[string]interface{}
func (s *Curd) ModId(modify map[string]interface{}, table interface{}, id interface{}) (int64, error) {
	return s.Mod(modify, table, ideq(), id)
}

// ModCtr update contrast, compare the values of before and after, the type of before or after should be AnyStruct, *AnyStruct, map[string]interface{}
func (s *Curd) ModCtr(before interface{}, after interface{}, table interface{}, where string, args ...interface{}) (int64, error) {
	var err error
	b, ok := before.(map[string]interface{})
	if !ok {
		err = s.JsonTransfer(before, &b)
		if err != nil {
			return 0, err
		}
	}
	a, ok := after.(map[string]interface{})
	if !ok {
		err = s.JsonTransfer(after, &a)
		if err != nil {
			return 0, err
		}
	}
	mod := map[string]interface{}{}
	for key, val := range a {
		beforeVal, ok := b[key]
		if !ok {
			continue
		}
		if reflect.DeepEqual(beforeVal, val) {
			continue
		}
		mod[key] = val
	}
	return s.Mod(mod, table, where, args...)
}

// ModCtrId update contrast, compare the values of before and after
func (s *Curd) ModCtrId(before interface{}, after interface{}, table interface{}, id interface{}) (int64, error) {
	return s.ModCtr(before, after, table, ideq(), id)
}

// Count sql count rows
func (s *Curd) Count(prepare string, args ...interface{}) (int64, error) {
	return s.hat.Count(prepare, args...)
}

// SumInt sql sum int
func (s *Curd) SumInt(prepare string, args ...interface{}) (int64, error) {
	return s.hat.SumInt(prepare, args...)
}

// SumFloat sql sum float
func (s *Curd) SumFloat(prepare string, args ...interface{}) (float64, error) {
	return s.hat.SumFloat(prepare, args...)
}

// Exists sql data exists
func (s *Curd) Exists(prepare string, args ...interface{}) (bool, error) {
	return s.hat.Exists(prepare, args...)
}
