package gormdm

import (
	"regexp"
	"strings"

	"gorm.io/gorm"
)

var (
	reg = regexp.MustCompile("`[a-zA-Z0-9_]+`")
)

func Raw(db *gorm.DB) {
	stmt := db.Statement
	sql := stmt.SQL.String()
	if strings.Contains(sql, "`") {
		// 正则查找是否有用``包裹的字段，替换成"且字段名替换成大写
		sql = reg.ReplaceAllStringFunc(sql, func(s string) string {
			return "\"" + strings.ToUpper(strings.Trim(s, "`")) + "\""
		})
		stmt.SQL.Reset()
		stmt.SQL.WriteString(sql)
	}
}
