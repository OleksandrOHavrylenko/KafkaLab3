package com.example;

import com.opencsv.*;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        logger.info("Sending history messages to kafka topic: {}.", this.getTopic());
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath)).withCSVParser(new RFC4180Parser()).build()) {
            List<String[]> linesToProduce = reader.readAll();
            linesToProduce.stream()
                    .skip(1)
                    .map(this::createProducerRecord)
                    .forEach(this::sendEvent);

            logger.info("Produced {} events to kafka topic: {}.", linesToProduce.size(), getTopic());
        } catch (IOException e) {
            logger.error("Error reading file {} due to ", filePath, e);
        } catch (CsvException e) {
            logger.error("csv file reading exception: ", e);
        } finally {
            logger.info("ProducerApp shutdown ");
            shutdown();
        }
    }

    private ProducerRecord<String, String> createProducerRecord(final String[] line) {
        if(line.length != 6){
            logger.error("Parsed history message: size={}, line: {}, ", line.length, line);
            return new ProducerRecord<>(this.dlq,"producer" , String.join(",", line));
        }
        return new ProducerRecord<>(this.topic, String.join(",", line));
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
