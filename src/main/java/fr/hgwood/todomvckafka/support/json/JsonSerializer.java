package fr.hgwood.todomvckafka.support.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Inspired by Spring Kafka
 * https://github.com/spring-projects/spring-kafka/blob/v2.0.0.M3/spring-kafka/src/main/java/org/springframework/kafka/support/serializer
 * http://www.apache.org/licenses/LICENSE-2.0
 */
public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper;

    public JsonSerializer(ObjectMapper objectMapper) {
        requireNonNull(objectMapper, "'objectMapper' must not be null");
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            byte[] result = null;
            if (data != null) {
                result = this.objectMapper.writeValueAsBytes(data);
            }
            return result;
        } catch (IOException ex) {
            throw new SerializationException(
                "Can't serialize data [" + data + "] for topic [" + topic + "]",
                ex
            );
        }
    }

    @Override
    public void close() {

    }
}
