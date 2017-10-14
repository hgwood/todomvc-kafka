package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.hgwood.todomvckafka.actions.DeriveFacts;
import fr.hgwood.todomvckafka.actions.todoitem.AddTodo;
import fr.hgwood.todomvckafka.facts.Assertion;
import fr.hgwood.todomvckafka.facts.EntityId;
import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.facts.FactRequest;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.json.JsonSerde2;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka.support.kafkastreams.TopologyTest;
import io.vavr.collection.HashSet;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;
import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.withRandomKey;
import static org.junit.Assert.assertEquals;

public class Pontificator2Test {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule()).registerModule(new JavaTimeModule());
    private static final Serde<EntityId> ENTITY_ID_SERDE = new JsonSerde<>(OBJECT_MAPPER, EntityId.class);
    private static final TopicInfo<String, Transaction<FactRequest>> FACT_REQUESTS = new TopicInfo<>("test-actions-topic",
        Serdes.String(),
        new JsonSerde2<>(OBJECT_MAPPER, new TypeReference<Transaction<FactRequest>>() {
        })
    );
    private static final TopicInfo<String, Transaction<Fact>> TRANSACTIONS = new TopicInfo<>(
        "test-todo-item-topic",
        Serdes.String(),
        new JsonSerde2<>(OBJECT_MAPPER, new TypeReference<Transaction<Fact>>() {
        })
    );
    private static final TopicInfo<String, Transaction<FactRequest>> REJECTED_TRANSACTIONS = new TopicInfo<>(
        "test-ignored-action-topic",
        Serdes.String(),
        new JsonSerde2<>(OBJECT_MAPPER, new TypeReference<Transaction<FactRequest>>() {
        })
    );
    private static final TopicInfo<EntityId, EntityId> ENTITY_EXISTS = new TopicInfo<>(
        "test-entity-exists-store",
        ENTITY_ID_SERDE,
        ENTITY_ID_SERDE
    );


    @Test
    public void addAction() throws Exception {
        /*String expectedTransactionId = "test-transaction-id";
        EntityId expectedEntityId = new EntityId("test-entity-id");
        Topology topology = new Pontificator2(FACT_REQUESTS,
            REJECTED_TRANSACTIONS,
            TRANSACTIONS,
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

            KeyValue<String, Transaction<FactRequest>> input = withRandomKey(new Transaction<>(new AddTodo(expectedText).accept(new DeriveFacts())));
            KeyValue<String, Transaction<Fact>> actual =
                topologyTest.write(FACT_REQUESTS, input).read(TRANSACTIONS).get();

            assertEquals(expected, actual);
        }*/
    }

    @Test
    public void deletes_an_existing_todo() throws Exception {
        /*String expectedTransactionId = "test-transaction-id";
        Topology topology =
            new Pontificator2(FACT_REQUESTS, REJECTED_TRANSACTIONS, TRANSACTIONS, ENTITY_EXISTS);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            EntityId entityToDelete = new EntityId("test-entity-id");
            KeyValue<String, Transaction> expected = KeyValue.pair(expectedTransactionId,
                new Transaction(HashSet.of(new EntityRetraction(entityToDelete)))
            );

            KeyValue<String, Action> add = withRandomKey(new AddTodo("test-todo-item-text"));
            KeyValue<String, Action> delete = withRandomKey(new DeleteTodo(entityToDelete.getValue()));
            KeyValue<String, Transaction> actual = topologyTest
                .write(FACT_REQUESTS, add)
                .write(FACT_REQUESTS, delete)
                .skip(TRANSACTIONS, 1)
                .read(TRANSACTIONS)
                .get();

            assertEquals(expected, actual);
        }*/
    }

    @Test
    public void ignores_delete_action_on_absent_todo() throws Exception {
        /*String expectedTransactionId = "test-transaction-id";
        Topology topology =
            new Pontificator2(FACT_REQUESTS, REJECTED_TRANSACTIONS, TRANSACTIONS, ENTITY_EXISTS);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String entityToDelete = "test-entity-id";
            Action expectedIgnoredAction = new DeleteTodo(entityToDelete);

            KeyValue<String, Action> delete = withRandomKey(expectedIgnoredAction);
            Option<KeyValue<String, Transaction>> transaction =
                topologyTest.write(FACT_REQUESTS, delete).read(TRANSACTIONS);
            Option<KeyValue<String, InvalidAction>> ignoredAction = topologyTest.read(REJECTED_TRANSACTIONS);

            assertFalse("expected no facts to be emitted but there was some",
                transaction.isDefined()
            );
            assertTrue(
                "expected the action to be ignored but it was not",
                ignoredAction.isDefined()
            );
            assertEquals(expectedIgnoredAction, ignoredAction.get().value);*/
    }

}
