package gormdm

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	_ "github.com/gotomicro/dmgo"
	"github.com/gotomicro/gorm-driver-dm/clauses"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	DriverName                string
	DSN                       string
	Conn                      gorm.ConnPool
	SkipInitializeWithVersion bool
	DefaultStringSize         uint
	DefaultDatetimePrecision  *int
	DisableDatetimePrecision  bool
	DontSupportRenameIndex    bool
	DontSupportRenameColumn   bool
	DontSupportForShareClause bool
}

type Dialector struct {
	*Config
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (d Dialector) Name() string {
	return "dm"
}

func (d Dialector) DummyTableName() string {
	return "DUAL"
}

func (d Dialector) Apply(config *gorm.Config) error {
	if config.NowFunc == nil {
		if d.DefaultDatetimePrecision == nil {
			var defaultDatetimePrecision = 3
			d.DefaultDatetimePrecision = &defaultDatetimePrecision
		}

		round := time.Second / time.Duration(math.Pow10(*d.DefaultDatetimePrecision))
		config.NowFunc = func() time.Time { return time.Now().Local().Round(round) }
	}
	return nil
}

func (d Dialector) Initialize(db *gorm.DB) (err error) {
	d.DefaultStringSize = 1024
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})

	if d.DriverName == "" {
		d.DriverName = "dm"
	}

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		db.ConnPool, err = sql.Open(d.DriverName, "dm://"+d.DSN)
		if err != nil {
			return err
		}
	}

	if err = db.Callback().Create().Replace("gorm:create", Create); err != nil {
		return
	}

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}

	return
}

func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:                          db,
				Dialector:                   d,
				CreateIndexAfterCreateTable: true,
			},
		},
	}
}

func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(`."`)
			}
			writer.WriteString(str)
			writer.WriteByte('"')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('"')
	}
}

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (d Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		sqlType := "bigint"
		switch {
		case field.Size <= 8:
			sqlType = "tinyint"
		case field.Size <= 16:
			sqlType = "smallint"
		case field.Size <= 24:
			sqlType = "mediumint"
		case field.Size <= 32:
			sqlType = "int"
		}

		if field.DataType == schema.Uint {
			sqlType += " unsigned"
		}

		if field.AutoIncrement {
			sqlType += " AUTO_INCREMENT"
		}
		return sqlType
	case schema.Float:
		if field.Precision > 0 {
			return fmt.Sprintf("decimal(%d, %d)", field.Precision, field.Scale)
		}

		if field.Size <= 32 {
			return "float"
		}
		return "double"
	case schema.String:
		size := field.Size
		defaultSize := d.DefaultStringSize

		if size == 0 {
			if defaultSize > 0 {
				size = int(defaultSize)
			} else {
				hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
				// TEXT, GEOMETRY or JSON column can't have a default value
				if field.PrimaryKey || field.HasDefaultValue || hasIndex {
					size = 191 // utf8mb4
				}
			}
		}

		if size >= 65536 && size <= int(math.Pow(2, 24)) {
			return "mediumtext"
		} else if size > int(math.Pow(2, 24)) || size <= 0 {
			return "longtext"
		}
		return fmt.Sprintf("varchar(%d)", size)
	case schema.Time:
		precision := ""

		if !d.DisableDatetimePrecision && field.Precision == 0 {
			field.Precision = *d.DefaultDatetimePrecision
		}

		if field.Precision > 0 {
			precision = fmt.Sprintf("(%d)", field.Precision)
		}

		if field.NotNull || field.PrimaryKey {
			return "datetime" + precision
		}
		return "datetime" + precision + " NULL"
	case schema.Bytes:
		if field.Size > 0 && field.Size < 65536 {
			return fmt.Sprintf("varbinary(%d)", field.Size)
		}

		if field.Size >= 65536 && field.Size <= int(math.Pow(2, 24)) {
			return "mediumblob"
		}

		return "longblob"
	}

	return string(field.DataType)
}

func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"LIMIT": d.RewriteLimit,
		"WHERE": d.RewriteWhere,
	}
}

func (d Dialector) RewriteLimit(c clause.Clause, builder clause.Builder) {
	if limit, ok := c.Expression.(clause.Limit); ok {
		if stmt, ok := builder.(*gorm.Statement); ok {
			if _, ok := stmt.Clauses["ORDER BY"]; !ok {
				s := stmt.Schema
				builder.WriteString("ORDER BY ")
				if s != nil && s.PrioritizedPrimaryField != nil {
					builder.WriteQuoted(s.PrioritizedPrimaryField.DBName)
					builder.WriteByte(' ')
				} else {
					builder.WriteString("(SELECT NULL FROM ")
					builder.WriteString(d.DummyTableName())
					builder.WriteString(")")
				}
			}
		}

		if offset := limit.Offset; offset > 0 {
			builder.WriteString(" OFFSET ")
			builder.WriteString(strconv.Itoa(offset))
			builder.WriteString(" ROWS")
		}
		if limit := limit.Limit; limit > 0 {
			builder.WriteString(" FETCH NEXT ")
			builder.WriteString(strconv.Itoa(limit))
			builder.WriteString(" ROWS ONLY")
		}
	}
}

func (d Dialector) RewriteWhere(c clause.Clause, builder clause.Builder) {
	if where, ok := c.Expression.(clause.Where); ok {
		builder.WriteString(" WHERE ")

		// Switch position if the first query expression is a single Or condition
		for idx, expr := range where.Exprs {
			if v, ok := expr.(clause.OrConditions); !ok || len(v.Exprs) > 1 {
				if idx != 0 {
					where.Exprs[0], where.Exprs[idx] = where.Exprs[idx], where.Exprs[0]
				}
				break
			}
		}

		wrapInParentheses := false
		for idx, expr := range where.Exprs {
			if idx > 0 {
				if v, ok := expr.(clause.OrConditions); ok && len(v.Exprs) == 1 {
					builder.WriteString(" OR ")
				} else {
					builder.WriteString(" AND ")
				}
			}

			if len(where.Exprs) > 1 {
				switch v := expr.(type) {
				case clause.OrConditions:
					if len(v.Exprs) == 1 {
						if e, ok := v.Exprs[0].(clause.Expr); ok {
							sql := strings.ToLower(e.SQL)
							wrapInParentheses = strings.Contains(sql, "and") || strings.Contains(sql, "or")
						}
					}
				case clause.AndConditions:
					if len(v.Exprs) == 1 {
						if e, ok := v.Exprs[0].(clause.Expr); ok {
							sql := strings.ToLower(e.SQL)
							wrapInParentheses = strings.Contains(sql, "and") || strings.Contains(sql, "or")
						}
					}
				case clause.Expr:
					sql := strings.ToLower(v.SQL)
					wrapInParentheses = strings.Contains(sql, "and") || strings.Contains(sql, "or")
				}
			}

			if wrapInParentheses {
				builder.WriteString(`(`)
				expr.Build(builder)
				builder.WriteString(`)`)
				wrapInParentheses = false
			} else {
				if e, ok := expr.(clause.IN); ok {
					if values, ok := e.Values[0].([]interface{}); ok {
						if len(values) > 1 {
							newExpr := clauses.IN{
								Column: expr.(clause.IN).Column,
								Values: expr.(clause.IN).Values,
							}
							newExpr.Build(builder)
							continue
						}
					}
				}

				expr.Build(builder)
			}
		}
	}
}
