package fr.hgwood.todomvckafka2.support.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Inspired by Spring Kafka
 * https://github.com/spring-projects/spring-kafka/blob/v2.0.0.M3/spring-kafka/src/main/java/org/springframework/kafka/support/serializer
 * http://www.apache.org/licenses/LICENSE-2.0
 */
public class JsonSerde2<T> implements Serde<T> {

    private final JsonSerializer<T> jsonSerializer;
    private final JsonDeserializer2<T> jsonDeserializer;

    public JsonSerde2(JsonSerializer<T> jsonSerializer, JsonDeserializer2<T> jsonDeserializer) {
        requireNonNull(jsonSerializer, "'jsonSerializer' must not be null.");
        requireNonNull(jsonDeserializer, "'jsonDeserializer' must not be null.");
        this.jsonSerializer = jsonSerializer;
        this.jsonDeserializer = jsonDeserializer;
    }

    public JsonSerde2(ObjectMapper objectMapper, TypeReference<T> targetType) {
        this(
            new JsonSerializer<>(objectMapper),
            new JsonDeserializer2<>(objectMapper, targetType)
        );
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.jsonSerializer.configure(configs, isKey);
        this.jsonDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.jsonSerializer.close();
        this.jsonDeserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return this.jsonSerializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this.jsonDeserializer;
    }

}
