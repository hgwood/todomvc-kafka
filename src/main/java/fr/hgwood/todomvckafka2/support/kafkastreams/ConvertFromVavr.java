package fr.hgwood.todomvckafka2.support.kafkastreams;

import io.vavr.Tuple2;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import org.apache.kafka.streams.KeyValue;


public class ConvertFromVavr {
    public static <K, V> KeyValue<K, V> toKeyValue(Tuple2<K, V> keyValue) {
        return KeyValue.pair(keyValue._1, keyValue._2);
    }

    public static <K, V> Seq<KeyValue<K, V>> toKeyValues(Map<K, V> keyValues) {
        return keyValues.map(ConvertFromVavr::toKeyValue);
    }
}
