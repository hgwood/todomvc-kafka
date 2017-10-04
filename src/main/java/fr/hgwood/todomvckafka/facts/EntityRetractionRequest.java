package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Value;

@Value
public class EntityRetractionRequest implements FactRequest {
    private final EntityLookup entity;

    @Override
    public Try<Option<Fact>> resolveEntity(EntityIdResolver resolver) {
        return resolver
            .resolve(entity)
            .map(maybeEntityId -> maybeEntityId.map(EntityRetraction::new));
    }
}
