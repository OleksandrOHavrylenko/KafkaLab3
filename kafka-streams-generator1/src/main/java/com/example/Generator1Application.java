package com.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;

@SpringBootApplication
public class Generator1Application {

    public static void main(String[] args) {
        SpringApplication.run(Generator1Application.class, args);
    }

    @Bean
    public ApplicationRunner runner(HistoryProducer historyProducer) {
        return args -> {
            historyProducer.produce();
        };
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(senderProps());
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv()
                .getOrDefault("BOOTSTRAP_SERVERS", "localhost:29092, localhost:39092, localhost:49092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(CLIENT_ID_CONFIG, System.getenv().getOrDefault("CLIENT_ID", "history-producer"));
        props.put(ACKS_CONFIG, System.getenv().getOrDefault("ACKS", "1"));
        return props;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
