package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.schema.Attribute;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka.support.kafkastreams.TopologyTest;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

public class FactsToDataTest {
    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule());
    private static final TopicInfo<String, Fact> FACTS = new TopicInfo(
        "test-facts-topic",
        Serdes.String(),
        new JsonSerde(OBJECT_MAPPER, Fact.class)
    );
    private static final TopicInfo<String, TodoItem> TODO_ITEMS = new TopicInfo(
        "test-todo-item-topic",
        Serdes.String(),
        new JsonSerde(OBJECT_MAPPER, TodoItem.class)
    );

    @Test
    public void singleAssertion() throws Exception {
        Topology topology =
            new FactsToDataTopology(FACTS, TODO_ITEMS, OBJECT_MAPPER);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedEntity = "test-entity-id";
            String expectedText = "test-todo-item-text-value";
            KeyValue<String, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, null));

            KeyValue<String, Fact> input =
                KeyValue.pair(randomUUID().toString(),
                    Fact.of(expectedEntity,
                        Attribute.TODO_ITEM_TEXT,
                        expectedText
                    )
                );
            topologyTest.write(FACTS, input);
            KeyValue<String, TodoItem> actual = topologyTest.read(TODO_ITEMS);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void twoAssertion() throws Exception {
        Topology topology =
            new FactsToDataTopology(FACTS, TODO_ITEMS, OBJECT_MAPPER);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedEntity = "test-entity-id";
            String expectedText = "test-todo-item-text-value";
            Boolean expectedCompletionState = true;
            KeyValue<String, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, expectedCompletionState));

            KeyValue<String, Fact> input1 =
                KeyValue.pair(randomUUID().toString(),
                    Fact.of(expectedEntity,
                        Attribute.TODO_ITEM_TEXT,
                        expectedText
                    )
                );
            topologyTest.write(FACTS, input1);
            KeyValue<String, Fact> input2 =
                KeyValue.pair(randomUUID().toString(),
                    Fact.of(expectedEntity,
                        Attribute.TODO_ITEM_COMPLETED,
                        expectedCompletionState
                    )
                );
            topologyTest.write(FACTS, input2);
            topologyTest.read(TODO_ITEMS); // ignored
            KeyValue<String, TodoItem> actual = topologyTest.read(TODO_ITEMS);

            assertEquals(expected, actual);
        }
    }
}
