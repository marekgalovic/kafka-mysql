package main

import (
  "github.com/marekgalovic/kafka-mysql"
)

func main() {
  config := kafkamysql.NewConfig()
  config.Parse()
  kafkamysql.Logger.Printf("Version: %s", kafkamysql.Version)

  consumer, err := kafkamysql.NewConsumer(config)
  if err != nil {
    kafkamysql.Logger.Fatal(err)
  }
  _, err = kafkamysql.NewLoader(config)
  if err != nil {
    kafkamysql.Logger.Fatal(err)
  }

  consumer.Batches(handleBatch)
}

func handleBatch(events [][]byte) error {
  return nil
}