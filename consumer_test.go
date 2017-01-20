package kafkamysql

import (
  "testing";

  "github.com/stretchr/testify/suite";
  "github.com/stretchr/testify/assert";
)

type ConsumerTestSuite struct {
  suite.Suite
  consumer *Consumer
}

func (suite *ConsumerTestSuite) SetupSuite() {
  config := NewConfig()
  consumer, err := NewConsumer(config)
  if err != nil {
    Logger.Fatal(err)
  }
  suite.consumer = consumer
}

func (suite *ConsumerTestSuite) TestBrokersReturnsAListOfBrokersIfSpecified() {
  suite.consumer.config.Kafka.Brokers = []string{"custombroker.com:9092"}

  brokers, err := suite.consumer.brokers()
  assert.Equal(suite.T(), []string{"custombroker.com:9092"}, brokers)
  assert.Nil(suite.T(), err)
}

func (suite *ConsumerTestSuite) TestBrokersFetchesBrokerListFromZookeeperIfBrokersAreNotSpecified() {
  suite.consumer.config.Kafka.Brokers = []string{}

  brokers, err := suite.consumer.brokers()
  assert.Equal(suite.T(), []string{"127.0.0.1:9092"}, brokers)
  assert.Nil(suite.T(), err)
}

func TestConsumerTestSuite(t *testing.T) {
  suite.Run(t, new(ConsumerTestSuite))
}