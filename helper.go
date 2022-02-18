package gomysql

import (
	"database/sql"
	"encoding/json"
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

// JsonTransfer by jsonMarshal and unmarshal transfer data from source to result
func JsonTransfer(source interface{}, result interface{}) (err error) {
	var bts []byte
	bts, err = json.Marshal(source)
	if err != nil {
		return
	}
	err = json.Unmarshal(bts, result)
	if err != nil {
		return
	}
	return
}

// Curd insert, update, delete, select
type Curd struct {
	hat *Hat
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

// Name0 mysql name to go name
func (s *Curd) Name0(name0 func(name string) string) {
	s.hat.name0 = name0
}

// Name1 go name to mysql name
func (s *Curd) Name1(name1 func(name string) string) {
	s.hat.name1 = name1
}

// Transaction closures execute transaction, err != nil auto rollback
func (s *Curd) Transaction(closure func(hat *Hat) (err error)) error {
	return s.hat.Transaction(closure)
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
func (s *Curd) PrepareArgs() (string, []interface{}) {
	return s.hat.PrepareArgs()
}

// Fetch execute any query sql, automatically match according to naming rules
func (s *Curd) Fetch(fetch interface{}, prepare string, args ...interface{}) error {
	return s.hat.Prepare(prepare).Args(args...).Fetch(fetch)
}

// GetOneStr get first one string
func (s *Curd) GetOneStr(prepare string, args ...interface{}) (map[string]*string, error) {
	return s.hat.Prepare(prepare).Args(args...).GetOneStr()
}

// GetAllStr get all string
func (s *Curd) GetAllStr(prepare string, args ...interface{}) ([]map[string]*string, error) {
	return s.hat.Prepare(prepare).Args(args...).GetAllStr()
}

// GetOneAny get first one any
func (s *Curd) GetOneAny(prepare string, args ...interface{}) (map[string]interface{}, error) {
	return s.hat.Prepare(prepare).Args(args...).GetOneAny()
}

// GetAllAny get all any
func (s *Curd) GetAllAny(prepare string, args ...interface{}) ([]map[string]interface{}, error) {
	return s.hat.Prepare(prepare).Args(args...).GetAllAny()
}

// JsonTransfer data exchange by json, map[string]interface{} => *AnyStruct , []map[string]interface{} => *[]AnyStruct | *[]*AnyStruct
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

// Insert execute an insert sql
func (s *Curd) Insert(prepare string, args ...interface{}) (int64, error) {
	return s.hat.Prepare(prepare).Args(args...).Create()
}

// IsStructPointer whether any is a struct pointer
func IsStructPointer(any interface{}) bool {
	if any == nil {
		return false
	}
	tp := reflect.TypeOf(any)
	if tp.Kind() != reflect.Ptr {
		return false
	}
	return tp.Elem().Kind() == reflect.Struct
}

// isStructPointer whether the interface is a struct pointer parameter
func (s *Curd) isStructPointer(any interface{}) bool {
	return IsStructPointer(any)
}

// InsertByMap by map[string]interface{} insert
func (s *Curd) InsertByMap(insert map[string]interface{}, table string) (id int64, err error) {
	if table == "" {
		err = errors.New("the insert table is empty")
		return
	}
	if insert == nil {
		err = errors.New("the insert object is nil")
		return
	}
	length := len(insert)
	columns := make([]string, length)
	values := make([]string, length)
	args := make([]interface{}, length)
	i := 0
	for key := range insert {
		columns[i] = key
		values[i] = "?"
		i++
	}
	// sort by field name
	sort.Strings(columns)
	for key, val := range columns {
		args[key] = insert[val]
	}
	prepare := fmt.Sprintf(
		"INSERT INTO %s ( %s ) VALUES ( %s );",
		Identifier(table),
		fmt.Sprintf("%s%s%s", Backtick, strings.Join(columns, fmt.Sprintf("%s, %s", Backtick, Backtick)), Backtick),
		strings.Join(values, ", "),
	)
	id, err = s.Insert(prepare, args...)
	return
}

// InsertByStruct insert by struct pointer
func (s *Curd) InsertByStruct(insert interface{}, table ...string) (id int64, err error) {
	if !s.isStructPointer(insert) {
		err = fmt.Errorf("the insert object is not a struct pointer")
		return
	}
	add := map[string]interface{}{}
	val := reflect.ValueOf(insert)
	vs := val.Elem()
	for i := 0; i < vs.NumField(); i++ {
		add[s.hat.name1(vs.Type().Field(i).Name)] = vs.Field(i).Interface()
	}
	tab := ""
	length := len(table)
	if length > 0 {
		tab = table[length-1]
	} else {
		tab = s.hat.name1(vs.Type().Name())
	}
	id, err = s.InsertByMap(add, tab)
	return
}

// Create insert data by struct pointer, auto set id value
func (s *Curd) Create(insert interface{}, table ...string) (err error) {
	if !s.isStructPointer(insert) {
		err = fmt.Errorf("the insert object is not a struct pointer")
		return
	}
	add := map[string]interface{}{}
	val := reflect.ValueOf(insert)
	vs := val.Elem()
	idi := -1
	tmp := ""
	for i := 0; i < vs.NumField(); i++ {
		tmp = s.hat.name1(vs.Type().Field(i).Name)
		add[tmp] = vs.Field(i).Interface()
		if tmp == "id" {
			idi = i
		}
	}
	tab := ""
	length := len(table)
	if length > 0 {
		tab = table[length-1]
	} else {
		tab = s.hat.name1(vs.Type().Name())
	}
	var id int64
	id, err = s.InsertByMap(add, tab)
	if err != nil {
		return
	}
	// set id value
	if idi >= 0 && id > 0 && vs.Field(idi).CanSet() && vs.Field(idi).Type().Kind() == reflect.Int64 {
		vs.Field(idi).SetInt(id)
	}
	return
}

// idEqual create sql `id` = ?
func (s *Curd) idEqual() string {
	return fmt.Sprintf("%s = ?", Identifier("id"))
}

// Delete delete by where
func (s *Curd) Delete(table string, where string, args ...interface{}) (int64, error) {
	if where == "" {
		return s.Execute(fmt.Sprintf("DELETE FROM %s;", Identifier(table)))
	}
	return s.Execute(fmt.Sprintf("DELETE FROM %s WHERE ( %s );", Identifier(table), where), args...)
}

// DeleteById delete by id
func (s *Curd) DeleteById(table string, id interface{}) (int64, error) {
	return s.Delete(table, s.idEqual(), id)
}

// UpdateByMap by map[string]interface{} update
func (s *Curd) UpdateByMap(update map[string]interface{}, table string, where string, args ...interface{}) (int64, error) {
	key, val := ModifyPrepareArgs(update)
	prepare := ""
	if where == "" {
		prepare = fmt.Sprintf("UPDATE %s SET %s;", Identifier(table), key)
	} else {
		prepare = fmt.Sprintf("UPDATE %s SET %s WHERE ( %s );", Identifier(table), key, where)
		val = append(val, args...)
	}
	return s.Execute(prepare, val...)
}

// UpdateByMapById by map[string]interface{} update
func (s *Curd) UpdateByMapById(modify map[string]interface{}, table string, id interface{}) (int64, error) {
	return s.UpdateByMap(modify, table, s.idEqual(), id)
}

// Update update table data based on two structure data
// before: source database data (struct pointer)
// after: the latest changed data (struct pointer)
func (s *Curd) Update(before interface{}, after interface{}, table string, where string, args ...interface{}) (rowsAffected int64, err error) {
	if !s.isStructPointer(before) {
		err = fmt.Errorf("the update object before is not a struct pointer")
		return
	}
	if !s.isStructPointer(after) {
		err = fmt.Errorf("the update object after is not a struct pointer")
		return
	}
	beforeValue := reflect.ValueOf(before)
	beforeValue1 := beforeValue.Elem()
	beforeMap := map[string]interface{}{}
	length := beforeValue1.NumField()
	for i := 0; i < length; i++ {
		beforeMap[beforeValue1.Type().Field(i).Name] = beforeValue1.Field(i).Interface()
	}
	afterValue := reflect.ValueOf(after)
	afterValue1 := afterValue.Elem()
	afterMap := map[string]interface{}{}
	length = afterValue1.NumField()
	for i := 0; i < length; i++ {
		afterMap[afterValue1.Type().Field(i).Name] = afterValue1.Field(i).Interface()
	}
	mod := map[string]interface{}{}
	for key, val := range afterMap {
		beforeVal, ok := beforeMap[key]
		if !ok {
			continue
		}
		if reflect.DeepEqual(beforeVal, val) {
			continue
		}
		mod[s.hat.name1(key)] = val
	}
	rowsAffected, err = s.UpdateByMap(mod, table, where, args...)
	return
}

// UpdateById update by id
func (s *Curd) UpdateById(before interface{}, after interface{}, table string, id interface{}) (int64, error) {
	return s.Update(before, after, table, s.idEqual(), id)
}

// Count statistics rows count
func (s *Curd) Count(prepare string, args ...interface{}) (count int64, err error) {
	err = s.Query(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			err = rows.Scan(&count)
		}
		return
	}, prepare, args...)
	return
}

// SumInt sql sum int64
func (s *Curd) SumInt(prepare string, args ...interface{}) (sum int64, err error) {
	err = s.Query(func(rows *sql.Rows) (err error) {
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
	}, prepare, args...)
	return
}

// SumFloat sql sum float64
func (s *Curd) SumFloat(prepare string, args ...interface{}) (sum float64, err error) {
	err = s.Query(func(rows *sql.Rows) (err error) {
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
	}, prepare, args...)
	return
}

// Exists check if data exists
func (s *Curd) Exists(prepare string, args ...interface{}) (exists bool, err error) {
	err = s.Query(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			exists = true
		}
		return
	}, prepare, args...)
	return
}
