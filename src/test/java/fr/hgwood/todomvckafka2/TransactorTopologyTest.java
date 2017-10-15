package fr.hgwood.todomvckafka2;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka2.actions.*;
import fr.hgwood.todomvckafka2.facts.Assertion;
import fr.hgwood.todomvckafka2.facts.EntityRetraction;
import fr.hgwood.todomvckafka2.facts.Transaction;
import fr.hgwood.todomvckafka2.reducers.Transactor;
import fr.hgwood.todomvckafka2.reducers.TransactorTopology;
import fr.hgwood.todomvckafka2.support.json.JsonSerde;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka2.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka2.support.kafkastreams.TopologyTest;
import io.vavr.control.Option;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import static fr.hgwood.todomvckafka2.support.kafkastreams.RandomKey.withRandomKey;
import static org.junit.Assert.*;

public class TransactorTopologyTest {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule());
    private static final TopicInfo<String, Action> ACTIONS = new TopicInfo<>("test-actions-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Action.class)
    );
    private static final TopicInfo<String, Transaction> TRANSACTIONS = new TopicInfo<>(
        "test-transactions-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, Transaction.class)
    );
    private static final TopicInfo<String, RejectedAction> REJECTED_ACTIONS = new TopicInfo<>(
        "test-rejected-action-topic",
        Serdes.String(),
        new JsonSerde<>(OBJECT_MAPPER, RejectedAction.class)
    );
    private static final TopicInfo<String, String> KNOWN_ENTITIES =
        new TopicInfo<>("test-known-entities-store", Serdes.String(), Serdes.String());

    @Test
    public void add_a_todo_then_delete_it_yields_a_retraction_of_the_added_todo() {
        Topology topology =
            new TransactorTopology(ACTIONS, TRANSACTIONS, REJECTED_ACTIONS, KNOWN_ENTITIES);
        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            AddTodo addTodo = new AddTodo("test-add-todo-text");
            Option<Transaction> addTodoTransaction = topologyTest
                .write(ACTIONS, withRandomKey(addTodo))
                .read(TRANSACTIONS)
                .map(kv -> kv.value);

            assertTrue(
                "expected ADD_TODO to succeed but it did not",
                addTodoTransaction.isDefined()
            );

            String addedTodoId =
                addTodoTransaction.get().getFacts().find(fact -> true).get().getEntity();
            DeleteTodo deleteTodo = new DeleteTodo(addedTodoId);
            Transaction deleteTodoTransaction = topologyTest
                .write(ACTIONS, withRandomKey(deleteTodo))
                .read(TRANSACTIONS)
                .get().value;

            assertTrue(deleteTodoTransaction
                .getFacts()
                .exists(fact -> fact instanceof EntityRetraction && fact
                    .getEntity()
                    .equals(addedTodoId)));
        }
    }

    @Test
    public void add_a_todo_then_edit_it_yields_a_todo_with_updated_text() {
        Topology topology =
            new TransactorTopology(ACTIONS, TRANSACTIONS, REJECTED_ACTIONS, KNOWN_ENTITIES);
        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            AddTodo addTodo = new AddTodo("test-add-todo-text");
            Option<Transaction> addTodoTransaction = topologyTest
                .write(ACTIONS, withRandomKey(addTodo))
                .read(TRANSACTIONS)
                .map(kv -> kv.value);

            assertTrue(
                "expected ADD_TODO to succeed but it did not",
                addTodoTransaction.isDefined()
            );

            String addedTodoId =
                addTodoTransaction.get().getFacts().find(fact -> true).get().getEntity();
            EditTodo editTodo = new EditTodo(addedTodoId, "test-edit-todo-text");
            Transaction deleteTodoTransaction = topologyTest
                .write(ACTIONS, withRandomKey(editTodo))
                .read(TRANSACTIONS)
                .get().value;

            assertTrue(deleteTodoTransaction
                .getFacts()
                .exists(fact -> fact instanceof Assertion && fact
                    .getEntity()
                    .equals(addedTodoId) && ((Assertion) fact).getValue().equals(editTodo.getText())));
        }
    }

    @Test
    public void delete_a_todo_that_does_not_exist_yields_an_unknown_entity_error() {
        Topology topology =
            new TransactorTopology(ACTIONS, TRANSACTIONS, REJECTED_ACTIONS, KNOWN_ENTITIES);
        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            DeleteTodo deleteTodo = new DeleteTodo("unknown-todo-id");

            Option<Transaction> deleteTodoTransaction = topologyTest
                .write(ACTIONS, withRandomKey(deleteTodo))
                .read(TRANSACTIONS)
                .map(kv -> kv.value);
            Option<RejectedAction> rejectedDeleteTodo =
                topologyTest.read(REJECTED_ACTIONS).map(kv -> kv.value);

            assertFalse("expected no transaction but there was one",
                deleteTodoTransaction.isDefined()
            );
            assertTrue("expected a rejected action but there was none",
                rejectedDeleteTodo.isDefined()
            );
            assertEquals(
                "expected the rejected action to be the input DELETE_TODO but it was not",
                deleteTodo,
                rejectedDeleteTodo.get().getAction()
            );
            assertEquals("expected rejection reason to be UNKNOWN_ENTITY but it was not",
                Transactor.RejectionMessages.unknownEntity(deleteTodo.getId()),
                rejectedDeleteTodo.get().getReason()
            );
        }
    }

}
