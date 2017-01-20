package kafkamysql

import (
  "os"
  "log";
  "fmt";
  "flag";
  "strings";
)

var Version = "0.1.0"
var Logger = log.New(os.Stdout, "[kafka-mysql] ", log.Flags())

func NewConfig() *Config {
  return &Config{
    Mysql: &MysqlConfig{
      Host: "127.0.0.1",
      Port: 3306,
      User: "root",
    },
    Kafka: &KafkaConfig{
      Brokers: []string{"127.0.0.1:9092"},
      Zookeepers: []string{"127.0.0.1:2181"},
      Topics: []string{""},
      FetchSize: 1048576,
    },
    ConnectionTimeout: 1,
    UpsertInterval: 2000,
    UpsertSize: 1000,
    MaxRetries: 3,
  }
}

type MysqlConfig struct {
  Host string
  Port int
  User string
  Password string
  Database string
  Table string
}

func (mc *MysqlConfig) connectionString() string {
  return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mc.User, mc.Password, mc.Host, mc.Port, mc.Database)
}

type KafkaConfig struct {
  Brokers []string
  Zookeepers []string
  Topics []string
  ConsumerGroup string
  InitialOffset string
  FetchSize int
}

type Config struct {
  BrokerList string
  ZookeeperList string
  TopicList string
  FieldList string
  Fields []string
  ConnectionTimeout int
  UpsertInterval int
  UpsertSize int
  MaxRetries int
  Mysql *MysqlConfig
  Kafka *KafkaConfig
}

func (c *Config) Parse() {
  c.Kafka.Brokers = strings.Split(c.BrokerList, ",")
  c.Kafka.Zookeepers = strings.Split(c.ZookeeperList, ",")
  c.Kafka.Topics = strings.Split(c.TopicList, ",")
  c.Fields = strings.Split(c.FieldList, ",")
}

func (c *Config) ParseFlags() {
  flag.StringVar(&c.BrokerList, "brokers", "127.0.0.1:9092", "Kafka brokers")
  flag.StringVar(&c.ZookeeperList, "zookeepers", "127.0.0.1:2181", "Zookeeper nodes")
  flag.StringVar(&c.TopicList, "topics", "", "Kafka topics")
  flag.StringVar(&c.Kafka.ConsumerGroup, "group", "", "Kafka consumer group name")
  flag.StringVar(&c.Kafka.InitialOffset, "initial-offset", "newest", "Kafka consumer initial offset")
  flag.IntVar(&c.Kafka.FetchSize, "fetch-size", 1048576, "Kafka consumer fetch size - bytes")
  flag.StringVar(&c.FieldList, "fields", "", "Event fields to import")
  flag.StringVar(&c.Mysql.Host, "mysql-host", "127.0.0.1", "Mysql host")
  flag.IntVar(&c.Mysql.Port, "mysql-port", 3306, "Mysql host")
  flag.StringVar(&c.Mysql.User, "mysql-user", "", "Mysql user")
  flag.StringVar(&c.Mysql.Password, "mysql-password", "", "Mysql password")
  flag.StringVar(&c.Mysql.Database, "mysql-database", "", "Mysql database")
  flag.StringVar(&c.Mysql.Table, "mysql-table", "", "Mysql table")
  flag.IntVar(&c.ConnectionTimeout, "timeout", 1, "Connection timeout")
  flag.IntVar(&c.UpsertInterval, "upsert-interval", 1000, "Time to wait before next upsert - miliseconds")
  flag.IntVar(&c.UpsertSize, "upsert-size", 1, "Number of events to upsert in one query")
  flag.IntVar(&c.MaxRetries, "max-retries", 3, "Retry count before skipping to next batch")
  flag.Parse()
  c.Parse()
}
