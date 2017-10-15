package fr.hgwood.todomvckafka2.support.kafkastreams;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
public class TopicInfo<K, V> {
    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
}
