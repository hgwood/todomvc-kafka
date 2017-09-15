package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.hgwood.todomvckafka.facts.EntityRetraction;
import fr.hgwood.todomvckafka.facts.ValueAssertion;
import fr.hgwood.todomvckafka.schema.Attribute;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka.support.kafkastreams.TopologyTest;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.withRandomKey;
import static org.junit.Assert.assertEquals;

public class EntityGathererTest {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule()).registerModule(new JavaTimeModule());
    private static final TopicInfo<String, Transaction> TRANSACTIONS = new TopicInfo<>(
        "test-transactions-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Transaction.class)
    );
    private static final TopicInfo<String, Map> ENTITIES = new TopicInfo<>(
        "test-todo-item-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Map.class)
    );
    private static final TopicInfo<String, TodoItem> TODO_ITEMS = new TopicInfo<>(
        "test-todo-item-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, TodoItem.class)
    );

    @Test
    public void singleAssertion() throws Exception {
        Topology topology = new EntityGatherer(TRANSACTIONS, ENTITIES);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedEntity = "test-entity-id";
            String expectedText = "test-todo-item-text-value";
            KeyValue<String, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, null));

            KeyValue<String, Transaction> input =
                withRandomKey(new Transaction(HashSet.of(new ValueAssertion<>(expectedEntity,
                    Attribute.TODO_ITEM_TEXT,
                    expectedText
                ))));
            KeyValue<String, TodoItem> actual =
                topologyTest.write(TRANSACTIONS, input).read(TODO_ITEMS);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void twoAssertion() throws Exception {
        Topology topology = new EntityGatherer(TRANSACTIONS, ENTITIES);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedEntity = "test-entity-id";
            String expectedText = "test-todo-item-text-value";
            Boolean expectedCompleted = true;
            KeyValue<String, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, expectedCompleted));

            KeyValue<String, Transaction> input =
                withRandomKey(new Transaction(HashSet.of(new ValueAssertion<>(expectedEntity,
                        Attribute.TODO_ITEM_TEXT,
                        expectedText
                    ),
                    new ValueAssertion<>(expectedEntity,
                        Attribute.TODO_ITEM_COMPLETED,
                        expectedCompleted
                    )
                )));
            KeyValue<String, TodoItem> actual =
                topologyTest.write(TRANSACTIONS, input).read(TODO_ITEMS);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void entityRetraction() throws Exception {
        Topology topology = new EntityGatherer(TRANSACTIONS, ENTITIES);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedEntity = "test-entity-id";
            KeyValue<String, TodoItem> expected = KeyValue.pair(expectedEntity, null);

            KeyValue<String, Transaction> input =
                withRandomKey(new Transaction(HashSet.of(new EntityRetraction(expectedEntity))));
            KeyValue<String, TodoItem> actual =
                topologyTest.write(TRANSACTIONS, input).read(TODO_ITEMS);

            assertEquals(expected, actual);
        }
    }
}
