package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.hgwood.todomvckafka.schema.Attribute;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka.support.kafkastreams.TopologyTest;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import static fr.hgwood.todomvckafka.Transaction.createTransaction;
import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.withRandomKey;
import static org.junit.Assert.assertEquals;

public class EntityGathererTest {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule()).registerModule(new JavaTimeModule());
    private static final TopicInfo<String, Fact> FACTS = new TopicInfo("test-facts-topic",
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
        Topology topology = new EntityGatherer(FACTS, TODO_ITEMS, OBJECT_MAPPER);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedEntity = "test-entity-id";
            String expectedText = "test-todo-item-text-value";
            KeyValue<String, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, null));

            KeyValue<String, Fact> input =
                withRandomKey(Fact.of(expectedEntity, Attribute.TODO_ITEM_TEXT, expectedText, createTransaction().getId()));
            KeyValue<String, TodoItem> actual = topologyTest.write(FACTS, input).read(TODO_ITEMS);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void twoAssertion() throws Exception {
        Topology topology = new EntityGatherer(FACTS, TODO_ITEMS, OBJECT_MAPPER);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            Transaction transaction = createTransaction();
            String expectedEntity = "test-entity-id";
            String expectedText = "test-todo-item-text-value";
            Boolean expectedCompleted = true;
            KeyValue<String, TodoItem> expected =
                KeyValue.pair(expectedEntity, new TodoItem(expectedText, expectedCompleted));

            KeyValue<String, Fact> textAssertion =
                withRandomKey(Fact.of(expectedEntity, Attribute.TODO_ITEM_TEXT, expectedText, transaction.getId()));
            KeyValue<String, Fact> completedAssertion = withRandomKey(Fact.of(expectedEntity,
                Attribute.TODO_ITEM_COMPLETED,
                expectedCompleted,
                transaction.getId()
            ));
            KeyValue<String, TodoItem> actual = topologyTest
                .write(FACTS, textAssertion)
                //.skip(TODO_ITEMS, 1)
                .write(FACTS, completedAssertion)
                .read(TODO_ITEMS);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void entityRetraction() throws Exception {
        Topology topology = new EntityGatherer(FACTS, TODO_ITEMS, OBJECT_MAPPER);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedEntity = "test-entity-id";
            KeyValue<String, TodoItem> expected = KeyValue.pair(expectedEntity, null);
            KeyValue<String, Fact> entityRetraction =
                withRandomKey(Fact.retractEntity(expectedEntity, createTransaction().getId()));
            KeyValue<String, TodoItem> actual =
                topologyTest.write(FACTS, entityRetraction).read(TODO_ITEMS);
            assertEquals(expected, actual);
        }
    }
}
