package kafkamysql

import (
  "fmt";
  "strings";
  "database/sql";

  _ "github.com/go-sql-driver/mysql"
)

func NewLoader(config *Config) (*Loader, error) {
  loader := &Loader{config: config}
  err := loader.init()
  if err != nil {
    return nil, err
  }
  return loader, nil
}

type Loader struct {
  config *Config
  db *sql.DB
}

func (l *Loader) init() error {
  db, err := sql.Open("mysql", l.config.Mysql.connectionString())
  if err != nil {
    return err
  }
  l.db = db

  Logger.Printf("Connected to MySql - host: %s:%d, user: %s, database: %s", l.config.Mysql.Host, l.config.Mysql.Port, l.config.Mysql.User, l.config.Mysql.Database)
  return nil
}

func (l *Loader) Upsert(rows [][]interface{}) (int64, error) {
  query := fmt.Sprintf(
    `INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s`,
    l.config.Mysql.Table,
    l.fields(),
    l.placeholders(len(rows)),
    l.duplicateKeyUpdateFields(),
  )

  stmt, err := l.db.Prepare(query)
  if err != nil {
    return 0, err
  }
  defer stmt.Close()

  result, err := stmt.Exec(l.values(rows)...)
  if err != nil {
    return 0, err
  }
  return result.RowsAffected()
}

func (l *Loader) fields() string {
  fields := []string{}
  for _, field := range l.config.Fields {
    fields = append(fields, fmt.Sprintf("`%s`", field))
  }
  return strings.Join(fields, ",")
}

func (l *Loader) duplicateKeyUpdateFields() string {
  fields := []string{}
  for _, field := range l.config.Fields {
    fields = append(fields, fmt.Sprintf("`%s`=VALUES(`%s`)", field, field))
  }
  return strings.Join(fields, ",")
}

func (l *Loader) placeholders(rowsCount int) string {
  rows := strings.Repeat(fmt.Sprintf("%s,", l.rowPlaceholders()), rowsCount)
  return rows[0:len(rows)-1]
}

func (l *Loader) rowPlaceholders() string {
  values := strings.Repeat("?,", len(l.config.Fields))
  return fmt.Sprintf("(%s)", values[0:len(values)-1])
}

func (l *Loader) values(rows [][]interface{}) []interface{} {
  values := []interface{}{}
  for _, row := range rows {
    values = append(values, row...)
  }
  return values
}