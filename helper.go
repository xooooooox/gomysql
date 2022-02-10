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

// modify 根据需要更新的map数据转化成字段切片和字段参数
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

// ModifyPrepareArgs 根据更新的map转化成sql更新脚本和对应参数
func ModifyPrepareArgs(update map[string]interface{}) (prepare string, args []interface{}) {
	var columns []string
	columns, args = modify(update)
	for key, val := range columns {
		columns[key] = fmt.Sprintf("%s = ?", Identifier(val))
	}
	prepare = strings.Join(columns, ", ")
	return
}

// JsonTransfer 通过json序列化和反序列化 把数据 source 转移到 result
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

// Transaction 必包执行事务, err != nil 自动回滚
func (s *Curd) Transaction(fc func(hat *Hat) (err error)) error {
	return s.hat.Transaction(fc)
}

// Begin 开起事务
func (s *Curd) Begin() error {
	return s.hat.Begin()
}

// Rollback 事务回滚
func (s *Curd) Rollback() error {
	return s.hat.Rollback()
}

// Commit 事务提交
func (s *Curd) Commit() error {
	return s.hat.Commit()
}

// Fetch 执行任何查询sql, 根据命名规则自动匹配
func (s *Curd) Fetch(any interface{}, prepare string, args ...interface{}) error {
	return s.hat.Prepare(prepare).Args(args...).Fetch(any)
}

// GetOneStr 查询第一条
func (s *Curd) GetOneStr(prepare string, args ...interface{}) (map[string]*string, error) {
	return s.hat.Prepare(prepare).Args(args...).GetOneStr()
}

// GetAllStr 查询所有
func (s *Curd) GetAllStr(prepare string, args ...interface{}) ([]map[string]*string, error) {
	return s.hat.Prepare(prepare).Args(args...).GetAllStr()
}

// GetOneAny 查询第一条
func (s *Curd) GetOneAny(prepare string, args ...interface{}) (map[string]interface{}, error) {
	return s.hat.Prepare(prepare).Args(args...).GetOneAny()
}

// GetAllAny 查询所有
func (s *Curd) GetAllAny(prepare string, args ...interface{}) ([]map[string]interface{}, error) {
	return s.hat.Prepare(prepare).Args(args...).GetAllAny()
}

// JsonTransfer data exchange by json, map[string]interface{} => *AnyStruct , []map[string]interface{} => *[]AnyStruct | *[]*AnyStruct
func (s *Curd) JsonTransfer(source interface{}, result interface{}) error {
	return JsonTransfer(source, result)
}

// Query 执行任何查询sql
func (s *Curd) Query(anonymous func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return s.hat.Scan(anonymous).Prepare(prepare).Args(args...).Query()
}

// Execute 执行任何非查询sql
func (s *Curd) Execute(prepare string, args ...interface{}) (int64, error) {
	return s.hat.Prepare(prepare).Args(args...).Execute()
}

// Insert 执行插入一条的sql
func (s *Curd) Insert(prepare string, args ...interface{}) (int64, error) {
	return s.hat.Prepare(prepare).Args(args...).Create()
}

// IsStructPointer 是否是一个结构体指针
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

// isStructPointer 接口是否是一个结构体指针参数
func (s *Curd) isStructPointer(any interface{}) bool {
	return IsStructPointer(any)
}

// InsertByMap 通过map[string]interface{}将数据插入数据库
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
	// 根据字段名称排序
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

// InsertByStruct 通过结构体指针将数据插入数据库, 可以指定表名, 表名以最后一个字符串为准, 不设置默认结构体名称转换
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

// Create 通过结构体指针将数据插入数据库自动设置自增长字段(bigint<=>int64)id值, 可以指定表名, 表名以最后一个字符串为准, 不设置默认结构体名称转换
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
	// 插入对象的结构体存在Id/ID字段, 插入行的自增长值是正整数, 结构体属性可以设置, 结构体属性类型为int64类型
	if idi >= 0 && id > 0 && vs.Field(idi).CanSet() && vs.Field(idi).Type().Kind() == reflect.Int64 {
		vs.Field(idi).SetInt(id)
	}
	return
}

// idEqual 创建 `id` = ? where 条件
func (s *Curd) idEqual() string {
	return fmt.Sprintf("%s = ?", Identifier("id"))
}

// Delete 删除表数据 使用where条件, where: DELETE FROM `table` WHERE ( where );
func (s *Curd) Delete(table string, where string, args ...interface{}) (int64, error) {
	if where == "" {
		return s.Execute(fmt.Sprintf("DELETE FROM %s;", Identifier(table)))
	}
	return s.Execute(fmt.Sprintf("DELETE FROM %s WHERE ( %s );", Identifier(table), where), args...)
}

// DeleteById 根据id字段删除数据, 通常id字段是自增长字段亦是数据库表主键
func (s *Curd) DeleteById(table string, id interface{}) (int64, error) {
	return s.Delete(table, s.idEqual(), id)
}

// UpdateByMap 通过map[string]interface{}更新数据
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

// UpdateByMapById 通过map[string]interface{}更新数据
func (s *Curd) UpdateByMapById(modify map[string]interface{}, table string, id interface{}) (int64, error) {
	return s.UpdateByMap(modify, table, s.idEqual(), id)
}

// Update 根据两个结构体数据更新表数据
// before:源数据库的数据(结构体指针)
// after:最新变化的数据(结构体指针)
// 更新字段为 before和after有相同名称的字段且字段值不相等,把对应字段的值设置成after对应字段的值, 不更新的字段不应该存在于after结构体中; 如果after结构体的类型和before类型是一致的, 建议设置成相同的值以避免数据剧库数据更新
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

// UpdateById 根据id更新
func (s *Curd) UpdateById(before interface{}, after interface{}, table string, id interface{}) (int64, error) {
	return s.Update(before, after, table, s.idEqual(), id)
}

// Count sql统计表的数据条数
func (s *Curd) Count(prepare string, args ...interface{}) (count int64, err error) {
	err = s.Query(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			err = rows.Scan(&count)
		}
		return
	}, prepare, args...)
	return
}

// SumInt sql int64 求和
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

// SumFloat sql float64 求和
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

// Exists sql查询数据是存在
func (s *Curd) Exists(prepare string, args ...interface{}) (exists bool, err error) {
	err = s.Query(func(rows *sql.Rows) (err error) {
		if rows.Next() {
			exists = true
		}
		return
	}, prepare, args...)
	return
}
