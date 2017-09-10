package fr.hgwood.todomvckafka;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestHarnessTest {

    private static final String TEMPLATE_TOPIC = "template-topic";

    private static final Serde<String> stringSerde = Serdes.String();

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TEMPLATE_TOPIC);


    @Test
    public void testHarnessStarts() throws Exception {
        Map<String, Object> producerProps =
            KafkaTestUtils.producerProps(embeddedKafka);
        ProducerFactory<String, String> producerFactory =
            new DefaultKafkaProducerFactory<>(producerProps, stringSerde.serializer(), stringSerde.serializer());
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
            "testGroup", "true", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(
            consumerProps, stringSerde.deserializer(), stringSerde.deserializer());
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, TEMPLATE_TOPIC);

        template.send(TEMPLATE_TOPIC, "foo");

        ConsumerRecord<String, String> reply = KafkaTestUtils.getSingleRecord(
            consumer, TEMPLATE_TOPIC);
        assertEquals("foo", reply.value());
    }

}
