package fr.hgwood.todomvckafka;

import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Map;

import static fr.hgwood.todomvckafka.ActionsToFacts.*;
import static fr.hgwood.todomvckafka.Fact.FactKind.ASSERTION;
import static fr.hgwood.todomvckafka.Fact.FactKind.RETRACTION;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.Assert.assertTrue;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.producerProps;

public class ActionsToFactsTest {

    private static final Serde<String> stringSerde = Serdes.String();

    @Rule
    public KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, ACTIONS_TOPIC, FACTS_TOPIC);


    @Test
    public void addAction() throws Exception {
        KafkaStreams streams = ActionsToFacts.actionsToFacts(embeddedKafka.getBrokersAsString());
        streams.start();

        Map<String, Object> producerProps = producerProps(embeddedKafka);
        ProducerFactory<String, Action> producerFactory = new DefaultKafkaProducerFactory<>(
            producerProps,
            stringSerde.serializer(),
            ACTION_SERDE.serializer()
        );
        KafkaTemplate<String, Action> template = new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps = consumerProps("testGroup", "false", embeddedKafka);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(consumerProps,
            stringSerde.deserializer(),
            FACT_SERDE.deserializer()
        );
        Consumer<String, Fact> consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, FACTS_TOPIC);

        Action inputAction =
            Action.builder().type(ActionType.ADD_TODO).text("test-add-todo-action-text").build();

        template.send(ACTIONS_TOPIC, inputAction);

        List<Fact> facts = Stream
            // this hard timeout makes the test fail if code is debugged with breakpoints
            .continually(() -> consumer.poll(1000))
            .takeUntil(records -> records.isEmpty())
            .flatMap(records -> records.records(FACTS_TOPIC))
            .map(ConsumerRecord::value)
            .toList();

        assertTrue("expected one entity to be created but there was multiple",
            facts.forAll(fact -> fact.getEntity().equals(facts.get(0).getEntity()))
        );
        assertTrue("expected one fact setting the text of the new todo but there was none",
            facts.existsUnique(fact -> fact.getKind().equals(ASSERTION)
                && fact.getAttribute().equals(Option.of(TODO_ITEM_TEXT))
                && fact.getValue().equals(Option.of(inputAction.getText())))
        );
        assertTrue("expected one fact setting the text of the new todo but there was none",
            facts.existsUnique(fact -> fact.getKind().equals(ASSERTION)
                && fact.getAttribute().equals(Option.of(TODO_ITEM_COMPLETED))
                && fact.getValue().equals(Option.of(false)))
        );

        consumer.close();
        streams.close();
    }

    @Test
    public void deleteAction() throws Exception {
        KafkaStreams streams = ActionsToFacts.actionsToFacts(embeddedKafka.getBrokersAsString());
        streams.start();

        Map<String, Object> producerProps = producerProps(embeddedKafka);
        ProducerFactory<String, Action> producerFactory = new DefaultKafkaProducerFactory<>(
            producerProps,
            stringSerde.serializer(),
            ACTION_SERDE.serializer()
        );
        KafkaTemplate<String, Action> template = new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps = consumerProps("testGroup", "false", embeddedKafka);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(consumerProps,
            stringSerde.deserializer(),
            FACT_SERDE.deserializer()
        );
        Consumer<String, Fact> consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, FACTS_TOPIC);

        Action inputAction =
            Action.builder().type(ActionType.DELETE_TODO).id("test-delete-action-id").build();

        template.send(ACTIONS_TOPIC, inputAction);

        List<Fact> facts = Stream
            // this hard timeout makes the test fail if code is debugged with breakpoints
            .continually(() -> consumer.poll(1000))
            .takeUntil(records -> records.isEmpty())
            .flatMap(records -> records.records(FACTS_TOPIC))
            .map(ConsumerRecord::value)
            .toList();

        assertTrue("expected one fact setting the text of the new todo but there was none",
            facts.existsUnique(fact -> fact.getKind().equals(RETRACTION)
                && fact.getEntity().equals(inputAction.getId()))
        );

        consumer.close();
        streams.close();
    }
}
