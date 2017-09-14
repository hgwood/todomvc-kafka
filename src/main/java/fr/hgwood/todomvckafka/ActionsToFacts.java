package fr.hgwood.todomvckafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.hgwood.todomvckafka.support.json.JsonSerde;
import io.vavr.collection.HashSet;
import io.vavr.jackson.datatype.VavrModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;
import static fr.hgwood.todomvckafka.support.kafkastreams.RandomKey.randomKey;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class ActionsToFacts {
    public static final String ACTIONS_TOPIC = "actions";
    public static final String TRANSACTIONS_TOPIC = "facts";
    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new VavrModule());
    public static final JsonSerde<Action> ACTION_SERDE =
        new JsonSerde<>(OBJECT_MAPPER, Action.class);
    public static final JsonSerde<Transaction> TRANSACTION_SERDE =
        new JsonSerde<>(OBJECT_MAPPER, Transaction.class);

    public static void main(String[] args) {
        KafkaStreams streams =
            actionsToFacts(System.getProperty("bootstrap.servers", "localhost:9092"));
        if (System.getProperty("streams.cleanUp", "false").equals("true")) {
            streams.cleanUp();
        }
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception e) {
                // ignored
            }
        }));
    }

    public static KafkaStreams actionsToFacts(String bootstrapServers) {
        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "actions-to-facts");
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        builder.stream(Serdes.String(), ACTION_SERDE, ACTIONS_TOPIC).mapValues(action -> {
            if (action.getType() == ActionType.ADD_TODO) {
                String entity = randomKey();
                return new Transaction(HashSet.of(
                    Fact.of(entity, TODO_ITEM_TEXT, action.getText()),
                    Fact.of(entity, TODO_ITEM_COMPLETED, false)
                ));
            } else if (action.getType() == ActionType.DELETE_TODO) {
                return new Transaction(HashSet.of(Fact.retractEntity(action.getId())));
            } else {
                return null;
            }
        }).to(Serdes.String(), TRANSACTION_SERDE, TRANSACTIONS_TOPIC);

        return new KafkaStreams(builder, config);
    }
}
