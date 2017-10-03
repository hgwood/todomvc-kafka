package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.collection.Map;
import lombok.Value;

@Value
public class ValueAssertion<T> implements Fact {
    private final TemporaryId entity;
    private final Attribute<T> attribute;
    private final T value;

    @Override
    public Map<String, Object> apply(Map<String, Object> entity) {
        return entity.put(this.attribute.getName(), this.value);
    }
}
