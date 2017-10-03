package fr.hgwood.todomvckafka.facts;

import lombok.Value;

@Value
public class TemporaryId implements EntityRef {
    private final String value;

    @Override
    public EntityId resolve(EntityIdResolver resolver) {
        return resolver.resolve(this);
    }
}
