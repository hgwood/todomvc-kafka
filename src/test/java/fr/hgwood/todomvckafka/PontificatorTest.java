package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.support.kafkastreams.TopicInfo;
import fr.hgwood.todomvckafka.support.kafkastreams.Topology;
import fr.hgwood.todomvckafka.support.kafkastreams.TopologyTest;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;
import org.springframework.kafka.core.*;

import java.util.Map;

import static fr.hgwood.todomvckafka.ActionType.ADD_TODO;
import static fr.hgwood.todomvckafka.ActionType.DELETE_TODO;
import static fr.hgwood.todomvckafka.ActionsToFacts.*;
import static fr.hgwood.todomvckafka.Fact.FactKind.RETRACTION;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;
import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.withRandomKey;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.kafka.test.utils.KafkaTestUtils.producerProps;

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


    @Test
    public void addAction() throws Exception {
        String expectedTransactionId = "test-transaction-id";
        String expectedEntityId = "test-entity-id";
        Topology topology = new Pontificator(ACTIONS,
            TRANSACTIONS,
            () -> expectedTransactionId,
            () -> expectedEntityId
        );

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String expectedText = "test-todo-item-text";
            KeyValue<String, Transaction> expected = KeyValue.pair(expectedTransactionId,
                new Transaction(HashSet.of(Fact.of(expectedEntityId, TODO_ITEM_TEXT, expectedText),
                    Fact.of(expectedEntityId, TODO_ITEM_COMPLETED, false)
                ))
            );

            KeyValue<String, Action> input =
                withRandomKey(new Action(ADD_TODO, null, expectedText));
            KeyValue<String, Transaction> actual =
                topologyTest.write(ACTIONS, input).read(TRANSACTIONS);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void deleteAction() throws Exception {
        String expectedTransactionId = "test-transaction-id";
        Topology topology =
            new Pontificator(ACTIONS, TRANSACTIONS, () -> expectedTransactionId, () -> null);

        try (TopologyTest topologyTest = new TopologyTest(topology)) {
            String entityToDelete = "test-entity-id";
            KeyValue<String, Transaction> expected = KeyValue.pair(
                expectedTransactionId,
                new Transaction(HashSet.of(Fact.retractEntity(entityToDelete)))
            );

            KeyValue<String, Action> input =
                withRandomKey(new Action(DELETE_TODO, entityToDelete, null));
            KeyValue<String, Transaction> actual =
                topologyTest.write(ACTIONS, input).read(TRANSACTIONS);

            assertEquals(expected, actual);
        }
    }
}
