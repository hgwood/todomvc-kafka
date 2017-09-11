package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.schema.Attribute;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.producerProps;

public class FactsToDataTest {
    private static final String applicationId = randomUUID().toString();
    private static final String intermediateTopic = applicationId
        + "-todo-items-aggregation-store";
    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule());
    private static final String FACTS_TOPIC = "test-facts-topic";
    private static final Serde<String> stringSerde = Serdes.String();
    private static final Serde<Fact> factSerde =
        new JsonSerde<>(OBJECT_MAPPER, Fact.class);
    private static final String TODO_ITEMS_TOPIC = "test-todo-item-topic";
    private static final Serde<TodoItem> TODO_ITEM_SERDE =
        new JsonSerde<>(OBJECT_MAPPER, TodoItem.class);

    @Rule
    public KafkaEmbedded embeddedKafka = new KafkaEmbedded(1,
        true,
        1,
        FACTS_TOPIC,
        TODO_ITEMS_TOPIC,
        intermediateTopic
    );


    @Test
    public void singleAssertion() throws Exception {
        KafkaStreams streams =
            FactsToData.factsToData(embeddedKafka.getBrokersAsString(),
                applicationId,
                FACTS_TOPIC,
                stringSerde,
                factSerde,
                TODO_ITEMS_TOPIC,
                stringSerde,
                TODO_ITEM_SERDE,
                OBJECT_MAPPER
            );
        streams.start();

        Map<String, Object> producerProps = producerProps(embeddedKafka);
        ProducerFactory<String, Fact> producerFactory =
            new DefaultKafkaProducerFactory<>(producerProps,
                stringSerde.serializer(),
                factSerde.serializer()
            );
        KafkaTemplate<String, Fact> template =
            new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps =
            consumerProps(randomUUID().toString(), "false", embeddedKafka);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(
            consumerProps,
            stringSerde.deserializer(),
            TODO_ITEM_SERDE.deserializer()
        );
        Consumer<String, TodoItem> consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, TODO_ITEMS_TOPIC);

        Fact fact = Fact.of("test-entity-id",
            Attribute.TODO_ITEM_TEXT,
            "test-todo-item-text-value"
        );

        template.send(FACTS_TOPIC, fact);

        List<KeyValue<String, TodoItem>> todoItems = Stream
            .continually(() -> consumer.poll(500))
            .flatMap(records -> records.records(TODO_ITEMS_TOPIC))
            .map(record -> KeyValue.pair(record.key(), record.value()))
            .take(1)
            .toList();

        assertEquals(1, todoItems.size());
        assertEquals(fact.getEntity(), todoItems.get(0).key);
        assertEquals(fact.getValue().get(), todoItems.get(0).value.getText());

        consumer.close();
        streams.close();
    }
}
