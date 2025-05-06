#!/bin/bash
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --replication-factor 3 --partitions 1 --create --topic history
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --replication-factor 3 --partitions 1 --create --topic dlq
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --replication-factor 3 --partitions 1 --create --topic domain-count
docker exec broker-1 ./opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --list