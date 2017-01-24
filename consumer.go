package kafkamysql

import (
  "fmt";
  "time";
  "strings";

  "github.com/Shopify/sarama";
  "github.com/bsm/sarama-cluster";
  "github.com/wvanbergen/kazoo-go";
)

func NewConsumer(config *Config) (*Consumer, error) {
  consumer := &Consumer{config: config}
  err := consumer.init()
  if err != nil {
    return nil, err
  }
  return consumer, nil
}

type Consumer struct {
  config *Config
  consumer *cluster.Consumer
  nextFlushAt time.Time
  batchMessages [][]byte
}

func (c *Consumer) init() error {
  brokers, err := c.brokers()
  if err != nil {
    return err
  }

  consumer, err := cluster.NewConsumer(brokers, c.config.Kafka.ConsumerGroup, c.config.Kafka.Topics, c.clusterConfig())
  if err != nil {
    return err
  }
  c.consumer = consumer

  Logger.Printf("Kafka consumer brokers: %s", strings.Join(brokers, ", "))
  Logger.Printf("Kafka consumer is ready - topics: %s, group: %s", strings.Join(c.config.Kafka.Topics, ", "), c.config.Kafka.ConsumerGroup)
  return nil
}

func (c *Consumer) brokers() ([]string, error) {
  if len(c.config.Kafka.Brokers) > 0 {
    return c.config.Kafka.Brokers, nil
  }
  if len(c.config.Kafka.Zookeepers) < 1 {
    return nil, fmt.Errorf("No zookeeper nodes specified.")
  }

  kazoo, err := kazoo.NewKazoo(c.config.Kafka.Zookeepers, kazoo.NewConfig())
  if err != nil {
    return nil, err
  }
  defer kazoo.Close()

  brokers, err := kazoo.BrokerList()
  if err != nil {
    return nil, err
  }
  return brokers, nil
}

func (c *Consumer) clusterConfig() *cluster.Config {
  clusterConfig := cluster.NewConfig()
  clusterConfig.Net.DialTimeout = time.Duration(c.config.ConnectionTimeout) * time.Second
  clusterConfig.Consumer.Offsets.Initial = c.initialOffset()
  clusterConfig.Consumer.Fetch.Default = int32(c.config.Kafka.FetchSize)
  clusterConfig.Consumer.Return.Errors = true
  clusterConfig.Group.Return.Notifications = true

  return clusterConfig
}

func (c *Consumer) initialOffset() int64 {
  if c.config.Kafka.InitialOffset == "newest" {
    return sarama.OffsetNewest
  }
  if c.config.Kafka.InitialOffset == "oldest" {
    return sarama.OffsetOldest
  }
  return sarama.OffsetNewest
}

func (c *Consumer) logErrors() {
  for err := range c.consumer.Errors() {
    Logger.Printf("Consumer error: %v", err)
  }
}

func (c *Consumer) logNotifications() {
  for notification := range c.consumer.Notifications() {
    Logger.Println("REBALANCED")
    for topic, partitions := range notification.Current {
      Logger.Printf("Topic: %s, partitions: %v", topic, partitions)
    }
  }
}

func (c *Consumer) Batches(handler func([][]byte) error) {
  go c.logErrors()
  go c.logNotifications()
  go func() {
    c.updateNextFlushTimestamp()
    for message := range c.consumer.Messages() {
      c.batchMessages = append(c.batchMessages, message.Value)
      c.consumer.MarkOffset(message, "")
      if c.shouldFlush() {
        err := c.handleWithRetries(handler)
        if err != nil {
          Logger.Printf("Batch skipped because handler error occured: %v", err)
        }
        c.consumer.CommitOffsets()
        Logger.Printf("Commited offset - topic: %s, partition: %d, offset: %d", message.Topic, message.Partition, message.Offset + 1)
        c.resetBatch()
      }
    }
  }()
}

func (c *Consumer) handleWithRetries(handler func([][]byte) error) error {
  retries := 0
  for {
    err := handler(c.batchMessages)
    if err != nil {
      if retries < c.config.MaxRetries {
        retries += 1
        continue
      }
      return err
    }
    break
  }
  return nil
}

func (c *Consumer) shouldFlush() bool {
  return len(c.batchMessages) >= c.config.UpsertSize || time.Now().After(c.nextFlushAt) || time.Now().Equal(c.nextFlushAt)
}

func (c *Consumer) updateNextFlushTimestamp() {
  c.nextFlushAt = time.Now().Add(time.Duration(c.config.UpsertInterval) * time.Millisecond)
}

func (c *Consumer) resetBatch() {
  c.updateNextFlushTimestamp()
  c.batchMessages = nil
}

func (c *Consumer) Close() error {
  return c.consumer.Close()
}