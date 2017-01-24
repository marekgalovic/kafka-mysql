package kafkamysql

import (
  "testing";

  "github.com/stretchr/testify/suite";
  "github.com/stretchr/testify/assert";
)

type LoaderTestSuite struct {
  suite.Suite
  loader *Loader
}

func (suite *LoaderTestSuite) SetupSuite() {
  config := NewConfig()
  config.Mysql.Database = "kafka_mysql_test"
  config.Mysql.Table = "events"
  config.Fields = []string{"foo", "amount"}
  loader, err := NewLoader(config)
  if err != nil {
    Logger.Fatal(err)
  }
  suite.loader = loader
}

func (suite *LoaderTestSuite) resetDb() {
  _, err := suite.loader.db.Exec("CREATE TABLE IF NOT EXISTS events(`id` INT(11) AUTO_INCREMENT PRIMARY KEY, `foo` VARCHAR(255), `amount` INT(11), UNIQUE KEY (`foo`));")
  if err != nil {
    Logger.Fatal(err)
  }
  _, err = suite.loader.db.Exec("DELETE FROM events;")
  if err != nil {
    Logger.Fatal(err)
  }
}

func (suite *LoaderTestSuite) countRows() int {
  rows, err := suite.loader.db.Query("SELECT count(*) as count FROM events;")
  if err != nil {
    Logger.Fatal(err)
  }
  var count int
  for rows.Next() {
    rows.Scan(&count)
  }
  rows.Close()
  return count
}

func (suite *LoaderTestSuite) TestFieldsReturnsCommaSeparatedFieldList() {
  expectedFields := "`foo`,`amount`"

  assert.Equal(suite.T(), expectedFields, suite.loader.fields())
}

func (suite *LoaderTestSuite) TestDuplicateKeyUpdateFieldsReturnCommaSeparatedFieldsToUpdate() {
  expectedFields := "`foo`=VALUES(`foo`),`amount`=VALUES(`amount`)"

  assert.Equal(suite.T(), expectedFields, suite.loader.duplicateKeyUpdateFields())
}

func (suite *LoaderTestSuite) TestRowPlaceholdersReturnsCorrectPlaceholdersForRow() {
  assert.Equal(suite.T(), "(?,?)", suite.loader.rowPlaceholders())
}

func (suite *LoaderTestSuite) TestPlaceholdersReturnsCorrectPlaceholdersForAllValues() {
  assert.Equal(suite.T(), "(?,?),(?,?),(?,?),(?,?),(?,?)", suite.loader.placeholders(5))
}

func (suite *LoaderTestSuite) TestValuesFlattensValuesToArray() {
  data := []map[string]interface{}{
    map[string]interface{}{"foo": "bar", "amount": 100, "extra": "field"},
    map[string]interface{}{"foo": "baz", "amount": 200, "extra": "field"},
  }
  expectedValues := []interface{}{"bar", 100, "baz", 200}

  assert.Equal(suite.T(), expectedValues, suite.loader.values(data))
}

func (suite *LoaderTestSuite) TestUpsertCreatesNewRowsAndUpdatesExistingOnes() {
  suite.resetDb()
  data := []map[string]interface{}{
    map[string]interface{}{"foo": "bar", "amount": 100},
    map[string]interface{}{"foo": "baz", "amount": 200},
  }
  _, err := suite.loader.Upsert(data)
  assert.Nil(suite.T(), err)
  assert.Equal(suite.T(), 2, suite.countRows())

  newData := []map[string]interface{}{
    map[string]interface{}{"foo": "bar", "amount": 200},
    map[string]interface{}{"foo": "barz", "amount": 300},
  }
  _, err = suite.loader.Upsert(newData)
  assert.Nil(suite.T(), err)
  assert.Equal(suite.T(), 3, suite.countRows())

  rows, err := suite.loader.db.Query("SELECT amount FROM events WHERE foo='bar' LIMIT 1")
  assert.Nil(suite.T(), err)
  var amount int
  for rows.Next() {
    rows.Scan(&amount)
  }
  rows.Close()
  assert.Equal(suite.T(), 200, amount)
}

func TestLoaderTestSuite(t *testing.T) {
  suite.Run(t, new(LoaderTestSuite))
}