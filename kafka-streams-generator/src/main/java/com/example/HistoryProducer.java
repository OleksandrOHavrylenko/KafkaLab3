package com.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * @author Oleksandr Havrylenko
 **/
@Component
public class HistoryProducer {
    private static final Logger logger = LoggerFactory.getLogger(HistoryProducer.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final String topic = System.getenv().getOrDefault("OUTPUT_TOPIC", "history");
    private final String dlq = System.getenv().getOrDefault("DEAD_LETTER_QUEUE", "dlq");
    private final String filePath = System.getenv().getOrDefault("INPUT_FILE_PATH", "input/history.csv");

    public void produce() {
        logger.info("Sending history messages to kafka topic: {}.", topic);
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath)).withCSVParser(new RFC4180Parser()).build()) {
            List<String[]> linesToProduce = reader.readAll();
            linesToProduce.stream()
                    .skip(1)
                    .forEach(this::sendEvent);

            logger.info("Produced {} events to kafka topic: {}.", linesToProduce.size(), topic);
        } catch (IOException e) {
            logger.error("Error reading file {} due to ", filePath, e);
        } catch (CsvException e) {
            logger.error("csv file reading exception: ", e);
        } finally {
            logger.info("ProducerApp Messages have been sent. ");
        }
    }

    private void sendEvent(final String[] line) {
        if(line.length != 6){
            logger.error("Parsed history message: size={}, line: {}, ", line.length, line);
            kafkaTemplate.send(this.dlq, "producer" , String.join(",", line));
        } else {
            kafkaTemplate.send(this.topic, String.join(",", line));
        }
    }
}
