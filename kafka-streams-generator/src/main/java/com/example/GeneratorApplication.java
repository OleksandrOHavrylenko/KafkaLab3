package com.example;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;

/**
 * @author Oleksandr Havrylenko
 **/
public class GeneratorApplication {
    public static void main(String[] args) {
        final Properties producerProperties = new Properties() {{
            put(BOOTSTRAP_SERVERS_CONFIG, System.getenv()
                    .getOrDefault("BOOTSTRAP_SERVERS", "broker-1:19092, broker-2:19092, broker-3:19092"));
            put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            put(CLIENT_ID_CONFIG, System.getenv().getOrDefault("CLIENT_ID", "history-producer"));
            put(ACKS_CONFIG, System.getenv().getOrDefault("ACKS", "1"));
        }};

        final ProducerHistory producerHistory = new ProducerHistory(producerProperties);
        producerHistory.produce();
    }
}