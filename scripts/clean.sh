#!/bin/bash
docker exec broker-1 ./opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --delete --group history-processor
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --delete --topic history
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --delete --topic dlq
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --delete --topic domain-count
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --delete --topic history-processor-domain-counts-store-changelog
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --delete --topic history-processor-domain-counts-store-repartition
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --list