> Tips: the json tag value is the database table field name value

```go

package main

import (
	"fmt"
	"os"

	mysql "github.com/xooooooox/gomysql"
)

func init(){
    err := mysql.Open("mysql", "username:password@tcp(127.0.0.1:3306)/test?charset=utf8mb4&collation=utf8mb4_unicode_ci")
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }
}

func main(){
    curd := mysql.NewCurd()
    first, err := curd.GetFirst("SELECT * FROM `user` ORDER BY `id` ASC LIMIT 0, 1;")
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        return
    }
    fmt.Printf("%#v\n", first)
    all, err := curd.GetAll("SELECT * FROM `user` ORDER BY `id` ASC LIMIT 0, 3;")
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        return
    }
    fmt.Printf("%#v\n", all)
}

```