package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Value;

@Value
public class AccretionRequest<T> implements FactRequest {
    private final EntityLookup entity;
    private final Attribute<T> attribute;
    private final T value;

    @Override
    public Try<Option<Fact>> resolveEntity(EntityIdResolver resolver) {
        return resolver
            .resolve(entity)
            .map(maybeEntity -> maybeEntity.map(entity -> new Assertion<>(entity,
                attribute,
                value
            )));
    }
}
