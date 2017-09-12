package fr.hgwood.todomvckafka.support.kafkastreams;

import org.apache.kafka.streams.kstream.KStreamBuilder;

@FunctionalInterface
public interface Topology {
    void build(KStreamBuilder builder);
}
