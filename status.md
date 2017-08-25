# Current status of Uncle K.

This page describes what works and what does not with the current state of the repository.

# High level features

Compatibility is aiming Apache Kafka 0.10.2.1

- [x] The API parsing front-end. Mostly done. Support for new calls and their versions can be added with a few lines of code
- [x] Producing to topics
- [x] Consuming from topics (HEAD, beginning, etc)
- [x] Compacted topics
- [ ] Data cleanup thread
- [ ] Consumer groups. Currently any consumer connecting is being told it is the only consumer.

# Client support

- [x] Default Kafka Java client
- [x] librdkafka (current master, tested on 7a2b80d5daeafca99c852b0406d64edc7267aefb)
- [ ] various older versions

# Planned improvements

- [ ] Custom backend with high throughput write support.
- [ ] Server stabiliy code (don't panic so much on errors)
- [ ] Protocol extensions to make performance better (e.g. empty response being empty)
- [ ] Admin UI

# Testing notes:

```
# Default java client tested like this:
./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test --property parse.key=true --property key.separator=:
./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic test --from-beginning --property print.key=true
./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic test --property print.key=true

# librdkafka tested using the latest build of kafkacat on top of latest build of the library
./kafkacat -P -b 127.0.0.1 -t test -c 2
./kafkacat -C -b 127.0.0.1 -t test
```
