# How to Run tests with Kafka cluster
### 1 Start Kafka cluster with the command from the root folder of the project.
```Shell
docker-compose -f docker-compose-kafka-kraft.yml up -d
```

### 2 Run script corresponding to the test

```Shell
 ./scripts/test.sh
```

### 3 Start docker-compose file to the test

```Shell
docker-compose -f docker-compose-test.yml up -d && docker-compose rm -f
```

### 4 Check the Metrics report during running the test.

```Shell
docker logs report-reddits --follow
```

### 5 Remove Kafka consumer groups and topics after test.

```Shell
./scripts/clean.sh
```
##### Tests
1. One producer, a topic with 1 partition, 1 consumer
2. One producer, a topic with 1 partition, 2 consumers
3. One producer, a topic with 2 partitions, 2 consumers
4. One producer, a topic with 5 partitions, 5 consumers
5. One producer, a topic with 10 partitions, 1 consumers
6. One producer, a topic with 10 partitions, 5 consumers
7. One producer, a topic with 10 partitions, 10 consumers
8. 2 producers (input data should be split into 2 parts), a topic with 10 partitions, 10 consumers