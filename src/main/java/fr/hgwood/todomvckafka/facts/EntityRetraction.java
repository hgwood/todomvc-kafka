package fr.hgwood.todomvckafka.facts;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import lombok.Value;

@Value
class EntityRetraction implements Fact {
    private final EntityId entity;

    @Override
    public Map<String, Object> apply(Map<String, Object> entity) {
        return HashMap.empty();
    }
}
