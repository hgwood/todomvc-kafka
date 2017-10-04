package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.actions.todoitem.AddTodo;
import fr.hgwood.todomvckafka.actions.todoitem.DeleteTodo;
import fr.hgwood.todomvckafka.facts.EntityRetraction;
import fr.hgwood.todomvckafka.facts.Assertion;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka.support.kafkastreams.TopologyTest;
import io.vavr.collection.HashSet;
import io.vavr.control.Option;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;
import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.withRandomKey;
import static org.junit.Assert.*;

public class PontificatorTest {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule()).registerModule(new JavaTimeModule());
    private static final TopicInfo<String, Action> ACTIONS = new TopicInfo<>("test-actions-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Action.class)
    );
    private static final TopicInfo<String, Transaction> TRANSACTIONS = new TopicInfo<>(
        "test-todo-item-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Transaction.class)
    );
    private static final TopicInfo<String, Action> IGNORED_ACTIONS = new TopicInfo<>(
        "test-ignored-action-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Action.class)
    );
    private static final TopicInfo<String, Boolean> ENTITY_EXISTS = new TopicInfo<>(
        "test-entity-exists-store",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Boolean.class)
    );


    @Test
    public void addAction() throws Exception {
        String expectedTransactionId = "test-transaction-id";
        String expectedEntityId = "test-entity-id";
        Topology topology = new Pontificator(ACTIONS,
            IGNORED_ACTIONS,
            TRANSACTIONS,
            () -> expectedTransactionId,
            () -> expectedEntityId,
            ENTITY_EXISTS
        );

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedText = "test-todo-item-text";
            KeyValue<String, Transaction> expected = KeyValue.pair(expectedTransactionId,
                new Transaction(HashSet.of(new Assertion<>(expectedEntityId,
                        TODO_ITEM_TEXT,
                        expectedText
                    ),
                    new Assertion<>(expectedEntityId, TODO_ITEM_COMPLETED, false)
                ))
            );

            KeyValue<String, Action> input = withRandomKey(new AddTodo(expectedText));
            KeyValue<String, Transaction> actual =
                topologyTest.write(ACTIONS, input).read(TRANSACTIONS).get();

            assertEquals(expected, actual);
        }
    }

    @Test
    public void deletes_an_existing_todo() throws Exception {
        String expectedTransactionId = "test-transaction-id";
        Topology topology =
            new Pontificator(ACTIONS, IGNORED_ACTIONS, TRANSACTIONS, () -> expectedTransactionId, () -> "test-entity-id", ENTITY_EXISTS);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String entityToDelete = "test-entity-id";
            KeyValue<String, Transaction> expected = KeyValue.pair(expectedTransactionId,
                new Transaction(HashSet.of(new EntityRetraction(entityToDelete)))
            );

            KeyValue<String, Action> add = withRandomKey(new AddTodo("test-todo-item-text"));
            KeyValue<String, Action> delete = withRandomKey(new DeleteTodo(entityToDelete));
            KeyValue<String, Transaction> actual = topologyTest
                .write(ACTIONS, add)
                .write(ACTIONS, delete)
                .skip(TRANSACTIONS, 1)
                .read(TRANSACTIONS)
                .get();

            assertEquals(expected, actual);
        }
    }

    @Test
    public void ignores_delete_action_on_absent_todo() throws Exception {
        String expectedTransactionId = "test-transaction-id";
        Topology topology =
            new Pontificator(ACTIONS, IGNORED_ACTIONS, TRANSACTIONS, () -> expectedTransactionId, () -> "test-entity-id", ENTITY_EXISTS);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String entityToDelete = "test-entity-id";
            Action expectedIgnoredAction = new DeleteTodo(entityToDelete);

            KeyValue<String, Action> delete = withRandomKey(expectedIgnoredAction);
            Option<KeyValue<String, Transaction>> transaction =
                topologyTest.write(ACTIONS, delete).read(TRANSACTIONS);
            Option<KeyValue<String, Action>> ignoredAction = topologyTest.read(IGNORED_ACTIONS);

            assertFalse("expected no facts to be emitted but there was some",
                transaction.isDefined()
            );
            assertTrue(
                "expected the action to be ignored but it was not",
                ignoredAction.isDefined()
            );
            assertEquals(expectedIgnoredAction, ignoredAction.get().value);
        }
    }
}
