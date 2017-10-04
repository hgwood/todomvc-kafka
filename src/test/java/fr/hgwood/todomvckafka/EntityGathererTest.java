package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.hgwood.todomvckafka.facts.EntityId;
import fr.hgwood.todomvckafka.facts.EntityRetraction;
import fr.hgwood.todomvckafka.facts.Assertion;
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

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;
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
    private static final TopicInfo<String, Map> ENTITIES = new TopicInfo<>("test-todo-item-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Map.class)
    );
    private static final TopicInfo<EntityId, TodoItem> TODO_ITEMS = new TopicInfo<>(
        "test-todo-item-topic",
        new JsonSerde<>(OBJECT_MAPPER, EntityId.class),
        new JsonSerde<>(OBJECT_MAPPER, TodoItem.class)
    );

    @Test
    public void singleAssertion() throws Exception {
        Topology topology = new EntityGatherer(TRANSACTIONS, ENTITIES);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            EntityId expectedEntity = new EntityId("test-entity-id");
            String expectedText = "test-todo-item-text-value";
            KeyValue<EntityId, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, null));

            KeyValue<String, Transaction> input =
                withRandomKey(new Transaction(HashSet.of(new Assertion<>(expectedEntity,
                    TODO_ITEM_TEXT,
                    expectedText
                ))));
            KeyValue<EntityId, TodoItem> actual =
                topologyTest.write(TRANSACTIONS, input).read(TODO_ITEMS).get();

            assertEquals(expected, actual);
        }
    }

    @Test
    public void twoAssertion() throws Exception {
        Topology topology = new EntityGatherer(TRANSACTIONS, ENTITIES);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            EntityId expectedEntity = new EntityId("test-entity-id");
            String expectedText = "test-todo-item-text-value";
            Boolean expectedCompleted = true;
            KeyValue<EntityId, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, expectedCompleted));

            KeyValue<String, Transaction> input =
                withRandomKey(new Transaction(HashSet.of(new Assertion<>(expectedEntity,
                        TODO_ITEM_TEXT,
                        expectedText
                    ),
                    new Assertion<>(expectedEntity,
                        TODO_ITEM_COMPLETED,
                        expectedCompleted
                    )
                )));
            KeyValue<EntityId, TodoItem> actual =
                topologyTest.write(TRANSACTIONS, input).read(TODO_ITEMS).get();

            assertEquals(expected, actual);
        }
    }

    @Test
    public void entityRetraction() throws Exception {
        Topology topology = new EntityGatherer(TRANSACTIONS, ENTITIES);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            EntityId expectedEntity = new EntityId("test-entity-id");
            KeyValue<EntityId, TodoItem> expected = KeyValue.pair(expectedEntity, null);

            KeyValue<String, Transaction> input =
                withRandomKey(new Transaction(HashSet.of(new EntityRetraction(expectedEntity))));
            KeyValue<EntityId, TodoItem> actual =
                topologyTest.write(TRANSACTIONS, input).read(TODO_ITEMS).get();

            assertEquals(expected, actual);
        }
    }
}
