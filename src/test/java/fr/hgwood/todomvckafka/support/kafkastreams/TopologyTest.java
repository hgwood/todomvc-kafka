package fr.hgwood.todomvckafka.support.kafkastreams;

import io.vavr.control.Option;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.ProcessorTopologyTestDriver;

import java.util.Properties;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class TopologyTest implements AutoCloseable {

    private final ProcessorTopologyTestDriver testDriver;

    public TopologyTest(Topology topology) {
        KStreamBuilder builder = new KStreamBuilder();
        topology.build(builder);

        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, randomUUID().toString());
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost");

        this.testDriver = new ProcessorTopologyTestDriver(new StreamsConfig(config), builder);
    }

    public <K, V> TopologyTest write(
        TopicInfo<K, V> topicInfo, KeyValue<K, V> payload
    ) {
        this.testDriver.process(
            topicInfo.getName(),
            payload.key,
            payload.value,
            topicInfo.getKeySerde().serializer(),
            topicInfo.getValueSerde().serializer()
        );
        return this;
    }

    public <K, V> Option<KeyValue<K, V>> read(TopicInfo<K, V> todoItems) {
        return Option.of(this.testDriver.readOutput(
            todoItems.getName(),
            todoItems.getKeySerde().deserializer(),
            todoItems.getValueSerde().deserializer()
        )).map(record -> KeyValue.pair(record.key(), record.value()));
    }

    public <K, V> TopologyTest skip(TopicInfo<K, V> todoItems, int count) {
        for (int i = 0; i < count; i++) {
            this.testDriver.readOutput(
                todoItems.getName(),
                todoItems.getKeySerde().deserializer(),
                todoItems.getValueSerde().deserializer()
            );
        }
        return this;
    }

    public void close() {
        this.testDriver.close();
    }
}
