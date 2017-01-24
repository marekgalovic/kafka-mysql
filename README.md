# Kafka -> Mysql [![Build Status](https://travis-ci.org/marekgalovic/kafka-mysql.svg?branch=master)](https://travis-ci.org/marekgalovic/kafka-mysql)
This package simplifies loading kafka events into mysql compliant databases. You can use it as a standalone cli tool or as a package within your application.

## CLI
```
./kafka-mysql --brokers=127.0.0.1:9092,127.0.0.2:9092 --group=test-consumer --topics=topic_a --fields=field_a,field_b --mysql-database=test --mysql-table=test_data
```
### Options
- **- -brokers** List of kafka brokers
- **- -zookeepers** List of zookeeper nodes (used to fetch kafka brokers if thery are not specified with --brokers parameter), default: `127.0.0.1:2181`
- **- -group** Consumer group name
- **- -topics** List of kafka topics
- **- -fields** List of fields you want to load to database
- **- -mysql-host** Mysql host, default: `127.0.0.1`
- **- -mysql-port** Mysql port, default: `3306`
- **- -mysql-user** Mysql user, default: `root`
- **- -mysql-password** Mysql password
- **- -mysql-database** Mysql database name
- **- -mysql-table** Mysql table name
- **- -upsert-interval** Mysql upstert query interval (milliseconds), default: `2000ms`
- **- -upsert-size** Number of events in one upsert query, default: `4000`
- **- -initial-offset** Initial consumer group offset [newest, oldest], default: `newest`
- **- -fetch-size** Kafka consumer default fetch size (bytes), default: `1MB`
- **- -connection-timeout** Kafka connection timeout (seconds), default: `1s`
- **- -max-retries** Number of retries if query goes wrong, default: `3`