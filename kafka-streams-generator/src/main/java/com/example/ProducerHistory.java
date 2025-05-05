package com.example;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author Oleksandr Havrylenko
 **/
public class ProducerHistory {
    private static final Logger logger = LoggerFactory.getLogger(ProducerHistory.class);

    private final Producer<String, String> producer;
    private final String topic = System.getenv().getOrDefault("OUTPUT_TOPIC", "history");
    private final String dlq = System.getenv().getOrDefault("DEAD_LETTER_QUEUE", "dlq");
    private final String filePath = System.getenv().getOrDefault("INPUT_FILE_PATH", "input/history.csv");

    public ProducerHistory(final Properties properties) {
        this.producer = new KafkaProducer<>(properties);;
    }

    public void produce() {
        try {
            logger.info("Sending history messages to kafka topic: {}.", this.getTopic());
            List<String> linesToProduce = Files.readAllLines(Paths.get(filePath));
            linesToProduce.stream()
                    .skip(1)
                    .map(this::createProducerRecord)
                    .forEach(this::sendEvent);

            logger.info("Produced {} events to kafka topic: {}.", linesToProduce.size(), getTopic());
        } catch (IOException e) {
            logger.error("Error reading file {} due to ", filePath, e);
        } finally {
            logger.info("ProducerApp shutdown ");
            shutdown();
        }
    }

    private ProducerRecord<String, String> createProducerRecord(final String line) {
        final String[] parts = line.split(",");
        if(parts.length != 6){
            logger.error("Parsed history message line from file: {} with error: {}", this.filePath , line);
            return new ProducerRecord<>(this.dlq,"producer" , line);
        }
        return new ProducerRecord<>(this.topic, line);
    }

    private Future<RecordMetadata> sendEvent(final ProducerRecord<String, String> record) {
        return producer.send(record);
    }

    private Future<RecordMetadata> sendEvent(final ProducerRecord<String, String> record, final Callback callback) {
        return producer.send(record, callback);
    }

    private String getTopic() {
        return topic;
    }

    private void shutdown() {
        producer.close();
    }
}
