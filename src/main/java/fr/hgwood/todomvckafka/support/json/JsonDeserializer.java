package fr.hgwood.todomvckafka.support.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Inspired by Spring Kafka
 * https://github.com/spring-projects/spring-kafka/blob/v2.0.0.M3/spring-kafka/src/main/java/org/springframework/kafka/support/serializer
 * http://www.apache.org/licenses/LICENSE-2.0
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectReader objectReader;

    // not asking for ObjectReader directly to ensure the type relationship
    // between the reader and the resulting JsonDeserializer instance.
    public JsonDeserializer(ObjectMapper objectMapper, Class<T> targetType) {
        requireNonNull(objectMapper, "'objectMapper' must not be null");
        this.objectReader = objectMapper.readerFor(targetType);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            T result = null;
            if (data != null) {
                result = this.objectReader.readValue(data);
            }
            return result;
        }
        catch (IOException e) {
            throw new SerializationException("Can't deserialize data [" + Arrays
                .toString(data) +
                "] from topic [" + topic + "]", e);
        }
    }

    @Override
    public void close() {

    }
}
