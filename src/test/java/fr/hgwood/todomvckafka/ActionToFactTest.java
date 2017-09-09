package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.support.json.JsonSerializer;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.springframework.kafka.test.utils.KafkaTestUtils.*;

public class ActionToFactTest {

    private static final String ACTIONS_TOPIC = "test-actions-topic";
    private static final String FACTS_TOPIC = "test-facts-topic";

    private static final Serde<String> stringSerde = Serdes.String();

    @ClassRule
    public static KafkaEmbedded embeddedKafka =
        new KafkaEmbedded(1, true, ACTIONS_TOPIC, FACTS_TOPIC);


    @Test
    public void addAction() throws Exception {
        Map<String, Object> producerProps = producerProps(embeddedKafka);
        ProducerFactory<String, Action> producerFactory =
            new DefaultKafkaProducerFactory<>(
                producerProps,
                stringSerde.serializer(),
                new JsonSerializer<>(new ObjectMapper())
            );
        KafkaTemplate<String, Action> template =
            new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps =
            consumerProps("testGroup", "true", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(
            consumerProps,
            stringSerde.deserializer(),
            stringSerde.deserializer()
        );
        Consumer<String, Fact> consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, ACTIONS_TOPIC);

        Action inputAction = Action
            .builder()
            .type(ActionType.ADD_TODO)
            .text("test-add-todo-action-text")
            .build();

        template.send(ACTIONS_TOPIC, inputAction);

        ConsumerRecords<String, Fact> reply = getRecords(consumer);
        Iterable<ConsumerRecord<String, Fact>> records =
            reply.records(FACTS_TOPIC);

        List<Fact> facts = List.ofAll(records).map(ConsumerRecord::value);
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
    }
}
