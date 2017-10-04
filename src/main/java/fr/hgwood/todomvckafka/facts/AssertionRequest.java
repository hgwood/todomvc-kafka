package fr.hgwood.todomvckafka.facts;

import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.control.Either;
import io.vavr.control.Option;
import lombok.Value;

@Value
public class AssertionRequest<T> implements FactRequest {
    private final TemporaryEntityId temporaryEntityId;
    private final Attribute<T> attribute;
    private final T value;

    @Override
    public Either<NoSuchEntity, Fact> resolveEntity(EntityIdResolver resolver) {
        return Either.right(new Assertion<>(resolver.resolve(temporaryEntityId), attribute, value));
    }
}
