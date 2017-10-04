package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.control.Either;
import io.vavr.control.Option;
import lombok.Value;

@Value
public class AccretionRequest<T> implements FactRequest {
    private final EntityLookup entityLookup;
    private final Attribute<T> attribute;
    private final T value;

    @Override
    public Either<NoSuchEntity, Fact> resolveEntity(EntityIdResolver resolver) {
        return resolver
            .resolve(entityLookup)
            .map(entityId -> new Assertion<>(entityId, attribute, value));
    }
}
