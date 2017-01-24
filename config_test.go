package kafkamysql

import (
  "testing";

  "github.com/stretchr/testify/assert";
)

func TestParse(t *testing.T) {
  config := NewConfig()
  config.brokerList = "broker,broker2,broker3"
  config.zookeeperList = "zookeeper,zookeeper2"
  config.topicList = "topic,another_topic"
  config.fieldList = "fielda,fieldb,fieldc"

  config.parse()

  assert.Equal(t, []string{"broker", "broker2", "broker3"}, config.Kafka.Brokers)
  assert.Equal(t, []string{"zookeeper", "zookeeper2"}, config.Kafka.Zookeepers)
  assert.Equal(t, []string{"topic", "another_topic"}, config.Kafka.Topics)
  assert.Equal(t, []string{"fielda", "fieldb", "fieldc"}, config.Fields)
}

func TestMysqlConnectionString(t *testing.T) {
  mysqlConfig := &MysqlConfig{Host: "127.0.0.1", Port: 3306, User: "root", Password: "toor", Database: "db"}

  assert.Equal(t, "root:toor@tcp(127.0.0.1:3306)/db", mysqlConfig.connectionString())
}