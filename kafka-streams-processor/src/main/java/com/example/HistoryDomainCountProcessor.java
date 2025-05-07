package com.example;

import com.opencsv.CSVParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author Oleksandr Havrylenko
 **/
@Component
public class HistoryDomainCountProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HistoryDomainCountProcessor.class);
    public static final String INPUT_TOPIC = "history";
    public static final String OUTPUT_TOPIC = "output-domain-counts";
    public static final String DOMAIN_COUNTS_STORE = "domain-counts";

    @Autowired
    private CSVParser parser;


    @Autowired
    void process(final StreamsBuilder streamsBuilder) {
        KStream<String, String> historyItemsStream = streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> wordCounts = historyItemsStream
                .mapValues(this::parseLine)
                .filter((key, value) -> value.length == 6)
                .mapValues(this::getHost)
                .mapValues(this::getTopLevelDomain)
                .peek((key, value) -> System.out.println("key: " + key + " value: " + value))
                .groupBy((key, domain) -> domain, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(DOMAIN_COUNTS_STORE));

        wordCounts.toStream().to(OUTPUT_TOPIC);
    }

    private String getTopLevelDomain(final String host) {
        final String[] domainNameParts = host.split("\\.");
        return domainNameParts[domainNameParts.length - 1];
    }

    private String[] parseLine(final String line) {
        try {
            return parser.parseLine(line);
        } catch (IOException e) {
            logger.error("CsvParser could not parse line", e);
            return new String[0];
        }
    }

    private String getHost(final String[] arr) {
        String historyUrl = arr[3];
        try {
            URL url = new URL(historyUrl);
            return url.getHost();
        } catch (MalformedURLException e) {
            logger.error("Could not create url from line: {}", historyUrl, e);
            return "";
        }
    }


}
