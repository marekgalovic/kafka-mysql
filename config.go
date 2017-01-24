package kafkamysql

import (
  "os"
  "log";
  "fmt";
  "flag";
  "strings";
  "io/ioutil";
  "encoding/json";
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
      Zookeepers: []string{"127.0.0.1:2181"},
      Topics: []string{""},
      FetchSize: 1048576,
    },
    ConnectionTimeout: 1,
    UpsertInterval: 2000,
    UpsertSize: 4000,
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
  Brokers       []string
  Zookeepers    []string
  Topics        []string
  ConsumerGroup string   `json:"consumer_group"`
  InitialOffset string   `json:"initial_offset"`
  FetchSize     int      `json:"fetch_size"`
}

type Config struct {
  ConfigFile        string
  brokerList        string
  zookeeperList     string
  topicList         string
  fieldList         string
  Fields            []string
  ConnectionTimeout int       `json:"connection_timeout"`
  UpsertInterval    int       `json:"upsert_interval"`
  UpsertSize        int       `json:"upsert_size"`
  MaxRetries        int       `json:"max_retries"`
  Mysql *MysqlConfig
  Kafka *KafkaConfig
}

func (c *Config) parse() {
  if c.brokerList != "" {
    c.Kafka.Brokers = strings.Split(c.brokerList, ",")
  }
  c.Kafka.Zookeepers = strings.Split(c.zookeeperList, ",")
  c.Kafka.Topics = strings.Split(c.topicList, ",")
  c.Fields = strings.Split(c.fieldList, ",")
}

func (c *Config) load() error {
  if c.ConfigFile == "" {
    return nil
  }

  data, err := ioutil.ReadFile(c.ConfigFile)
  if err != nil {
    return err
  }
  return json.Unmarshal(data, &c)
}

func (c *Config) ParseFlags() error {
  flag.StringVar(&c.ConfigFile, "conf", "", "Config file")
  flag.StringVar(&c.brokerList, "brokers", "", "Kafka brokers")
  flag.StringVar(&c.zookeeperList, "zookeepers", "127.0.0.1:2181", "Zookeeper nodes")
  flag.StringVar(&c.topicList, "topics", "", "Kafka topics")
  flag.StringVar(&c.Kafka.ConsumerGroup, "group", "", "Kafka consumer group name")
  flag.StringVar(&c.Kafka.InitialOffset, "initial-offset", "newest", "Kafka consumer initial offset")
  flag.IntVar(&c.Kafka.FetchSize, "fetch-size", 1048576, "Kafka consumer fetch size - bytes")
  flag.StringVar(&c.fieldList, "fields", "", "Event fields to import")
  flag.StringVar(&c.Mysql.Host, "mysql-host", "127.0.0.1", "Mysql host")
  flag.IntVar(&c.Mysql.Port, "mysql-port", 3306, "Mysql host")
  flag.StringVar(&c.Mysql.User, "mysql-user", "root", "Mysql user")
  flag.StringVar(&c.Mysql.Password, "mysql-password", "", "Mysql password")
  flag.StringVar(&c.Mysql.Database, "mysql-database", "", "Mysql database")
  flag.StringVar(&c.Mysql.Table, "mysql-table", "", "Mysql table")
  flag.IntVar(&c.ConnectionTimeout, "timeout", 1, "Connection timeout")
  flag.IntVar(&c.UpsertInterval, "upsert-interval", 2000, "Time to wait before next upsert - milliseconds")
  flag.IntVar(&c.UpsertSize, "upsert-size", 4000, "Number of events to upsert in one query")
  flag.IntVar(&c.MaxRetries, "max-retries", 3, "Retry count before skipping to next batch")
  flag.Parse()

  c.parse()
  return c.load()
}
