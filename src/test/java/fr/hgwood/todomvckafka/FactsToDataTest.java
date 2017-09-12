package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.schema.Attribute;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import fr.hgwood.todomvckafka.utils.kafkastreams.TopicInfo;
import io.vavr.jackson.datatype.VavrModule;
import kafka.security.auth.Topic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.junit.Test;

import java.util.Properties;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.Assert.assertEquals;

public class FactsToDataTest {
    private static final String applicationId = randomUUID().toString();
    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule());
    private static final String FACTS_TOPIC = "test-facts-topic";
    private static final Serde<String> stringSerde = Serdes.String();
    private static final Serde<Fact> factSerde =
        new JsonSerde<>(OBJECT_MAPPER, Fact.class);
    private static final String TODO_ITEMS_TOPIC = "test-todo-item-topic";
    private static final Serde<TodoItem> TODO_ITEM_SERDE =
        new JsonSerde<>(OBJECT_MAPPER, TodoItem.class);

    @Test
    public void singleAssertion() throws Exception {
        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, applicationId);
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost");
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        FactsToData.buildFactsToData(kStreamBuilder,
            FACTS_TOPIC,
            stringSerde,
            factSerde,
            TODO_ITEMS_TOPIC,
            stringSerde,
            TODO_ITEM_SERDE,
            OBJECT_MAPPER
        );
        ProcessorTopologyTestDriver processorTopologyTestDriver =
            new ProcessorTopologyTestDriver(new StreamsConfig(config),
                kStreamBuilder
            );

        String expectedEntity = "test-entity-id";
        String expectedText = "test-todo-item-text-value";
        String inputFactKey = randomUUID().toString();
        Fact inputFact =
            Fact.of(expectedEntity, Attribute.TODO_ITEM_TEXT, expectedText);

        processorTopologyTestDriver.process(FACTS_TOPIC,
            inputFactKey,
            inputFact,
            stringSerde.serializer(),
            factSerde.serializer()
        );

        ProducerRecord<String, TodoItem> actual =
            processorTopologyTestDriver.readOutput(TODO_ITEMS_TOPIC,
                stringSerde.deserializer(),
                TODO_ITEM_SERDE.deserializer()
            );

        assertEquals(expectedEntity, actual.key());
        assertEquals(expectedText, actual.value().getText());
        processorTopologyTestDriver.close();
    }
}
