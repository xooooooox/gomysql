package gomysql

import (
	"sort"
)

func Modify(modify map[string]interface{}) (columns []string, args []interface{}) {
	length := len(modify)
	if length == 0 {
		return
	}
	columns = make([]string, length, length)
	args = make([]interface{}, length, length*2)
	i := 0
	for key := range modify {
		columns[i] = key
		i++
	}
	sort.Strings(columns)
	for key, val := range columns {
		args[key] = modify[val]
	}
	return
}
