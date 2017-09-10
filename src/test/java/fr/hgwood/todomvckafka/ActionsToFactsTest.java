package fr.hgwood.todomvckafka;

import io.vavr.collection.List;
import io.vavr.collection.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Map;

import static fr.hgwood.todomvckafka.ActionsToFacts.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.Assert.assertTrue;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.producerProps;

public class ActionsToFactsTest {

    private static final Serde<String> stringSerde = Serdes.String();

    @ClassRule
    public static KafkaEmbedded embeddedKafka =
        new KafkaEmbedded(1, true, 1, ACTIONS_TOPIC, FACTS_TOPIC);


    @Test
    public void addAction() throws Exception {
        KafkaStreams streams =
            ActionsToFacts.actionsToFacts(embeddedKafka.getBrokersAsString());
        streams.start();

        Map<String, Object> producerProps = producerProps(embeddedKafka);
        ProducerFactory<String, Action> producerFactory =
            new DefaultKafkaProducerFactory<>(
                producerProps,
                stringSerde.serializer(),
                ACTION_SERDE.serializer()
            );
        KafkaTemplate<String, Action> template =
            new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps =
            consumerProps("testGroup", "false", embeddedKafka);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(
            consumerProps,
            stringSerde.deserializer(),
            FACT_SERDE.deserializer()
        );
        Consumer<String, Fact> consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, FACTS_TOPIC);

        Action inputAction = Action
            .builder()
            .type(ActionType.ADD_TODO)
            .text("test-add-todo-action-text")
            .build();

        template.send(ACTIONS_TOPIC, inputAction);

        List<Fact> facts = Stream
            // this hard timeout makes the test fail if code is debugged with breakpoints
            .continually(() -> consumer.poll(1000))
            .takeUntil(records -> records.isEmpty())
            .flatMap(records -> records.records(FACTS_TOPIC))
            .map(ConsumerRecord::value)
            .toList();

        assertTrue(
            "all facts not about the same entity",
            facts.forAll(fact -> fact
                .getEntity()
                .equals(facts.get(0).getEntity()))
        );
        assertTrue(
            "no fact about text of add action",
            facts.existsUnique(fact -> fact.getAttribute().equals("text")
                && fact.getValue().equals(inputAction.getText()))
        );

        consumer.close();
        streams.close();
    }
}
