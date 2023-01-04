package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Dialector struct {
	*Config
}

type Config struct {
	DriverName           string
	DSN                  string
	PreferSimpleProtocol bool
	WithoutReturning     bool
	Conn                 gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{&Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return "ydb"
}

var timeZoneMatcher = regexp.MustCompile("(time_zone|TimeZone)=(.*?)($|&| )")

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	if !dialector.WithoutReturning {
		callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
			CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
			UpdateClauses: []string{"UPDATE", "SET", "WHERE", "RETURNING"},
			DeleteClauses: []string{"DELETE", "FROM", "WHERE", "RETURNING"},
		})
	}

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else if dialector.DriverName != "" {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.Config.DSN)
	} else {
		nativeDriver, err := ydb.Open(context.TODO(), dialector.Config.DSN, ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN"))) // See many ydb.Option's for configure driver https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3#Option
		if err != nil {
			return err
			// fallback on error
		}
		defer nativeDriver.Close(context.TODO())
		connector, err := ydb.Connector(nativeDriver) // See ydb.ConnectorOption's for configure connector https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3#ConnectorOption
		if err != nil {
			return err
		}
		defer connector.Close()
		db.ConnPool = sql.OpenDB(connector)
	}
	return
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('$')
	writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '`':
			continuousBacktick++
			if continuousBacktick == 2 {
				writer.WriteString("``")
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				writer.WriteByte('`')
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				writer.WriteByte('`')
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				writer.WriteString("``")
			}

			writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		writer.WriteString("``")
	}
	writer.WriteByte('`')
}

var numericPlaceholder = regexp.MustCompile(`\$(\d+)`)

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "Bool"
	case schema.Int, schema.Uint:
		size := field.Size
		if field.DataType == schema.Uint {
			size++
		}
		if field.AutoIncrement {
			switch {
			case size <= 16:
				return "Int8"
			case size <= 32:
				return "Int32"
			default:
				return "Int64"
			}
		} else {
			switch {
			case size <= 16:
				return "Int8"
			case size <= 32:
				return "Int32"
			default:
				return "Int64"
			}
		}
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("numeric(%d, %d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("numeric(%d)", field.Precision)
		}
		return "decimal"
	case schema.String:
		if field.Size > 0 {
			return fmt.Sprintf("varchar(%d)", field.Size)
		}
		return "text"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("Timestamp(%d)", field.Precision)
		}
		return "Timestamp"
	case schema.Bytes:
		return "bytea"
	default:
		return dialector.getSchemaCustomType(field)
	}
}

func (dialector Dialector) getSchemaCustomType(field *schema.Field) string {
	sqlType := string(field.DataType)

	if field.AutoIncrement && !strings.Contains(strings.ToLower(sqlType), "Int32") {
		size := field.Size
		if field.GORMDataType == schema.Uint {
			size++
		}
		switch {
		case size <= 16:
			sqlType = "Int8"
		case size <= 32:
			sqlType = "Int32"
		default:
			sqlType = "Int64"
		}
	}

	return sqlType
}

func (dialector Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dialector Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}

func getSerialDatabaseType(s string) (dbType string, ok bool) {
	switch s {
	case "Int8":
		return "smallint", true
	case "Int32":
		return "integer", true
	case "Int64":
		return "bigint", true
	default:
		return "", false
	}
}
