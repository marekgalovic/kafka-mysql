package main

import (
  "github.com/marekgalovic/kafka-mysql"
)

func main() {
  config := kafkamysql.NewConfig()
  config.ParseFlags()
  kafkamysql.Logger.Printf("Version: %s", kafkamysql.Version)

  consumer, err := kafkamysql.NewConsumer(config)
  if err != nil {
    kafkamysql.Logger.Fatal(err)
  }
  loader, err := kafkamysql.NewLoader(config)
  if err != nil {
    kafkamysql.Logger.Fatal(err)
  }

  consumer.Batches(func(events [][]byte) error {
    data, err := kafkamysql.ParseJson(events)
    if err != nil {
      return err
    }
    rowsAffected, err := loader.Upsert(data)
    if err != nil {
      return err
    }
    kafkamysql.Logger.Printf("Affected rows: %d", rowsAffected)
    return nil
  })
}