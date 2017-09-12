package fr.hgwood.todomvckafka.support.kafkastreams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.ProcessorTopologyTestDriver;

import java.util.Properties;
import java.util.function.Consumer;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class TopologyTest {

    private final ProcessorTopologyTestDriver testDriver;

    private TopologyTest(Topology topology) {
        KStreamBuilder builder = new KStreamBuilder();
        topology.build(builder);

        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, randomUUID().toString());
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost");

        this.testDriver =
            new ProcessorTopologyTestDriver(new StreamsConfig(config), builder);
    }

    public static void topologyTest(
        Topology topology, Consumer<TopologyTest> test
    ) {
        TopologyTest topologyTest = new TopologyTest(topology);
        test.accept(topologyTest);
        topologyTest.close();
    }

    public <K, V> void write(
        TopicInfo<K, V> topicInfo, KeyValue<K, V> payload
    ) {
        this.testDriver.process(
            topicInfo.getName(),
            payload.key,
            payload.value,
            topicInfo.getKeySerde().serializer(),
            topicInfo.getValueSerde().serializer()
        );
    }

    public <K, V> KeyValue<K, V> read(TopicInfo<K, V> todoItems) {
        ProducerRecord<K, V> record = this.testDriver.readOutput(
            todoItems.getName(),
            todoItems.getKeySerde().deserializer(),
            todoItems.getValueSerde().deserializer()
        );
        return KeyValue.pair(record.key(), record.value());
    }

    private void close() {
        this.testDriver.close();
    }
}
