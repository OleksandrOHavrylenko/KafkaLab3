package com.example;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.example.HistoryDomainCountProcessor.DOMAIN_COUNTS_STORE;

/**
 * @author Oleksandr Havrylenko
 **/
@RestController
public class HistoryDomainController {

    final private StreamsBuilderFactoryBean factoryBean;

    private final Map<String, Long> domainCounts = new HashMap<>();

    public HistoryDomainController(final StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @GetMapping("/topFiveDomains")
    public ResponseEntity<Map<String, Long>> getWordCount() {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(DOMAIN_COUNTS_STORE, QueryableStoreTypes.keyValueStore())
        );
        KeyValueIterator<String, Long> all = counts.all();
        while (all.hasNext()) {
            KeyValue<String, Long> next = all.next();
            domainCounts.put(next.key, next.value);
        }
        all.close();

        Map<String, Long> topFiveDomains = domainCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(5)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        return ResponseEntity.ok(topFiveDomains);
    }
}
