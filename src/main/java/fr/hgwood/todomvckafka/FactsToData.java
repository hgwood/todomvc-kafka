package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import io.vavr.collection.HashMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class FactsToData {

    public static KafkaStreams factsToData(
        String bootstrapServers,
        String applicationId,
        String factsTopic,
        Serde<String> factKeySerde,
        Serde<Fact> factSerde,
        String todoItemsTopic,
        Serde<String> todoItemKeySerde,
        Serde<TodoItem> todoItemSerde,
        ObjectMapper objectMapper
    ) {
        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, applicationId);
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        buildFactsToData(
            builder,
            factsTopic,
            factKeySerde,
            factSerde,
            todoItemsTopic,
            todoItemKeySerde,
            todoItemSerde,
            objectMapper
        );
        return new KafkaStreams(builder, config);
    }

    public static void buildFactsToData(
        KStreamBuilder builder,
        String factsTopic,
        Serde<String> factKeySerde,
        Serde<Fact> factSerde,
        String todoItemsTopic,
        Serde<String> todoItemKeySerde,
        Serde<TodoItem> todoItemSerde,
        ObjectMapper objectMapper
    ) {
        builder
            .stream(factKeySerde, factSerde, factsTopic)
            .groupBy((factKey, fact) -> fact.getEntity(),
                Serdes.String(),
                factSerde
            )
            .aggregate(() -> HashMap.<String, Object>empty(),
                (entityKey, fact, entity) -> entity.put(fact
                    .getAttribute()
                    .get()
                    .getName(), fact.getValue().get()),
                new JsonSerde<>(objectMapper, HashMap.class),
                "todo-items-aggregation-store"
            )
            .mapValues(value -> objectMapper.convertValue(value,
                TodoItem.class
            ))
            .to(todoItemKeySerde, todoItemSerde, todoItemsTopic);
    }
}

