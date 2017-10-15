package fr.hgwood.todomvckafka2.support.kafkastreams;

import org.apache.kafka.streams.kstream.KStreamBuilder;

@FunctionalInterface
public interface Topology {
    static Topology compose(Topology... topologies) {
        return builder -> {
            for (Topology topology : topologies) {
                topology.build(builder);
            }
        };
    }

    void build(KStreamBuilder builder);
}
