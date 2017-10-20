package fr.hgwood.todomvckafka2;

import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka2.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopologyTest;
import io.vavr.control.Option;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Test;

import static fr.hgwood.todomvckafka2.support.kafkastreams.RandomKey.withRandomKey;

public class KTableStoreInProcessorTest {
    private static final TopicInfo<String, String> TABLE_SOURCE =
        new TopicInfo<>("test-table-source-topic", Serdes.String(), Serdes.String());
    private static final TopicInfo<String, String> TABLE_SINK =
        new TopicInfo<>("test-table-sink-topic", Serdes.String(), Serdes.String());
    private static final TopicInfo<String, String> PROCESSOR_SOURCE =
        new TopicInfo<>("test-processor-source-topic", Serdes.String(), Serdes.String());
    private static final TopicInfo<String, String> PROCESSOR_SINK =
        new TopicInfo<>("test-processor-sink-topic", Serdes.String(), Serdes.String());

    @Test
    public void test() {
        Topology topology = new Topology() {

            @Override
            public void build(KStreamBuilder builder) {
                builder.table(
                    TABLE_SOURCE.getKeySerde(),
                    TABLE_SOURCE.getValueSerde(),
                    TABLE_SOURCE.getName(),
                    "storeName"
                )
                .to(TABLE_SINK.getKeySerde(), TABLE_SINK.getValueSerde(), TABLE_SINK.getName());

                builder.addSource(
                    "source",
                    PROCESSOR_SOURCE.getKeySerde().deserializer(),
                    PROCESSOR_SOURCE.getValueSerde().deserializer(),
                    PROCESSOR_SOURCE.getName()
                );
                builder.addProcessor(
                    "processorName",
                    () -> new AbstractProcessor<String, String>() {
                        private KeyValueStore<String, String> store;

                        @Override
                        public void init(ProcessorContext context) {
                            super.init(context);
                            this.store =
                                (KeyValueStore<String, String>) context.getStateStore("storeName");
                        }

                        @Override
                        public void process(String key, String value) {
                            if (this.store.get(key) != null) {
                                this.context().forward(key, value);
                            }
                        }

                        @Override
                        public void punctuate(long timestamp) {

                        }

                        @Override
                        public void close() {

                        }
                    },
                    "source"
                );
                builder.connectProcessorAndStateStores("processorName", "storeName");
                builder.addSink(
                    "sink",
                    PROCESSOR_SINK.getName(),
                    PROCESSOR_SINK.getKeySerde().serializer(),
                    PROCESSOR_SINK.getValueSerde().serializer(),
                    "processorName"
                );
            }
        };
        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            topologyTest
                .write(PROCESSOR_SOURCE, withRandomKey("ignored"))
                .write(TABLE_SOURCE, KeyValue.pair("key1", "valueA"))
                .write(PROCESSOR_SOURCE, KeyValue.pair("key1", "valueZ"))
                .write(TABLE_SOURCE, KeyValue.pair("key1", "valueB"))
                .write(PROCESSOR_SOURCE, KeyValue.pair("key2", "valueY"))
                .write(TABLE_SOURCE, KeyValue.pair("key2", "valueC"))
                .write(PROCESSOR_SOURCE, KeyValue.pair("key2", "valueX"));
            Option<KeyValue<String, String>> o = Option.of(KeyValue.pair("a", "b"));
            while (o.isDefined()) {
                o = topologyTest.read(TABLE_SINK);
                System.out.println(o);
            }

        }
    }
}
