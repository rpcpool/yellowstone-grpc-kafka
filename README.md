# Yellowstone gRPC Kafka Tool

Forward gRPC stream to Kafka, dedup, read stream from Kafka with gRPC server.

Modes:

- `grpc2kafka` — connect to gRPC with specified filter and sent all incoming messages to the Kafka
- `dedup` — consume messages from Kafka and sent deduplicated messages to another topic (right now only support `memory` as deduplication backend)
- `kafka2grpc` — provide gRPC endpoint with sending messages from Kafka

```bash
$ cargo run --bin grpc-kafka -- --help
Yellowstone gRPC Kafka Tool

Usage: grpc-kafka [OPTIONS] --config <CONFIG> <COMMAND>

Commands:
  dedup       Receive data from Kafka, deduplicate and send them back to Kafka
  grpc2kafka  Receive data from gRPC and send them to the Kafka
  kafka2grpc  Receive data from Kafka and send them over gRPC
  help        Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG>          Path to config file
      --prometheus <PROMETHEUS>  Prometheus listen address
  -h, --help                     Print help
  -V, --version                  Print version
```

##### Development

```bash
# run kafka locally
docker-compose -f docker-kafka.yml up
# create topic
kafka_2.13-3.5.0/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --topic grpc1
# send messages from gRPC to Kafka
cargo run --bin grpc-kafka -- --config config-kafka.json grpc2kafka
# read messages from Kafka
kafka_2.13-3.5.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic grpc1
```
