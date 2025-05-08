# How to Run history domain counter project
### 0 Prepare correct history.csv file. 
Put correct history.csv file to /input directory.
history.csv file format should be like /input/history-example.csv


### 1 Start Kafka cluster with the command from the root folder of the project.
```Shell
docker-compose -f docker-compose-kafka-kraft.yml up -d
```

### 2 Run script corresponding to the test

```Shell
./scripts/init.sh
```

### 3 Start docker-compose file to run history producer and consumer

```Shell
docker-compose -f compose.yml up -d --build && docker-compose rm -f
```

### 4 Check the result of top five domains from the history.csv file an endpoint.

```Shell
curl -X GET http://localhost:8080/topFiveDomains
```

### 5 Stop history producer and consumer

```Shell
docker-compose -f compose.yml down
```

### 6 Remove Kafka consumer groups and topics after test.

```Shell
./scripts/clean.sh
```
### 7 Stop Kafka cluster.
```Shell
docker-compose -f docker-compose-kafka-kraft.yml down
```