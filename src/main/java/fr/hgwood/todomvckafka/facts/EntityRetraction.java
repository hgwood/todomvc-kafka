package fr.hgwood.todomvckafka.facts;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import lombok.Value;

@Value
public class EntityRetraction implements Fact {
    private final String entity;

    @Override
    public Map<String, Object> apply(Map<String, Object> entity) {
        return HashMap.empty();
    }
}
