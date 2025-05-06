package com.example;

import com.google.common.io.Files;
import com.opencsv.CSVParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * @author Oleksandr Havrylenko
 **/
public class ProcessorApplication {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorApplication.class);
    static final String inputTopic = "history";
    static final String outputTopic = "domain-count";
    static CSVParser parser = new CSVParser();

    public static void main(String[] args) {

        final Properties streamsProperties = new Properties() {{

            put(StreamsConfig.APPLICATION_ID_CONFIG, "domain-statistic-1");
            put(StreamsConfig.CLIENT_ID_CONFIG, "domain-statistic-client");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv()
                    .getOrDefault("BOOTSTRAP_SERVERS", "localhost:29092, localhost:39092, localhost:49092"));
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
            put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            // Use a temporary directory for storing state, which will be automatically removed after the test.
            put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDir().getAbsolutePath());
        }};

        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);
//        streams.cleanUp();
        streams.start();
//        ReadOnlyKeyValueStore<String, Long> keyValueStore =
//                streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
    static void createWordCountStream(final StreamsBuilder builder) {
        // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).  The default key and value serdes will be used.
        final KStream<String, String> historyLines = builder.stream(inputTopic);

        final KTable<String, Long> topLevelDomainCount = historyLines
                .mapValues(ProcessorApplication::parseLine)
                .filter((key, value) -> value.length == 6)
//                .peek((key, value) -> System.out.println("key: "+ key + ", value: " + value))
                .mapValues(arr -> getHost(arr))
                .mapValues(host -> getTopLevelDomain(host))
//                .selectKey((key, domain) -> domain)
//                .peek((key, value) -> System.out.println("key: "+ key + ", value: " + value))
//                .groupByKey()
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        topLevelDomainCount.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }

    private static String getTopLevelDomain(String host) {
        String[] domainNameParts = host.split("\\.");
        return domainNameParts[domainNameParts.length-1];
    }

    private static String getHost(String[] arr) {
        try {
            URL url = new URL(arr[3]);
            return url.getHost();
        } catch (MalformedURLException e) {
            logger.error("Could not create url from line: {}", arr[3], e);
            return "";
        }
    }

    private static String[] parseLine(String line) {
        try {
            return parser.parseLine(line);

        } catch (IOException e) {
            logger.error("CsvParser could not parse line", e);
            return new String[0];
        }
    }

}