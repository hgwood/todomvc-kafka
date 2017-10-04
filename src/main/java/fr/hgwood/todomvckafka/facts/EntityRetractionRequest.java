package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Either;
import lombok.Value;

@Value
public class EntityRetractionRequest implements FactRequest {
    private final EntityLookup entityLookup;

    @Override
    public Either<NoSuchEntity, Fact> resolveEntity(EntityIdResolver resolver) {
        return resolver.resolve(entityLookup).map(EntityRetraction::new);
    }
}
