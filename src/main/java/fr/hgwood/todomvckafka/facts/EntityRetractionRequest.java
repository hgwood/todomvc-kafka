package fr.hgwood.todomvckafka.facts;

import lombok.Value;

@Value
public class EntityRetractionRequest implements FactRequest {
    private final EntityLookup entity;

    @Override
    public Fact resolveEntity(EntityIdResolver resolver) {
        return new EntityRetraction(resolver.resolve(entity));
    }
}
