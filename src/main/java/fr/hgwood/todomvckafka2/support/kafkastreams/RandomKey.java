package fr.hgwood.todomvckafka2.support.kafkastreams;

import org.apache.kafka.streams.KeyValue;

import static java.util.UUID.randomUUID;

public class RandomKey {
    public static String randomKey() {
        return randomUUID().toString();
    }

    public static <V> KeyValue<String, V> withRandomKey(V value) {
        return KeyValue.pair(randomKey(), value);
    }
}
