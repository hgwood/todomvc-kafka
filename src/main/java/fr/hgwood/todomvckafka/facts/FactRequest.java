package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;

public interface FactRequest {
    Either<NoSuchEntity, Fact> resolveEntity(EntityIdResolver resolver);
}
