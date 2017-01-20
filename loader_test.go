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
  config.Mysql.Table = "test_table"
  config.Fields = []string{"foo", "amount"}
  loader, err := NewLoader(config)
  if err != nil {
    Logger.Fatal(err)
  }
  suite.loader = loader
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

func TestLoaderTestSuite(t *testing.T) {
  suite.Run(t, new(LoaderTestSuite))
}