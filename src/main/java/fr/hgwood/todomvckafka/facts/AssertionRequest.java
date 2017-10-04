package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Value;

@Value
public class AssertionRequest<T> implements FactRequest {
    private final TemporaryEntityId entity;
    private final Attribute<T> attribute;
    private final T value;

    @Override
    public Try<Option<Fact>> resolveEntity(EntityIdResolver resolver) {
        return Try.success(Option.of(new Assertion<>(resolver.resolve(entity), attribute, value)));
    }
}
