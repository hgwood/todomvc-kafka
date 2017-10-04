package fr.hgwood.todomvckafka.facts;

import io.vavr.control.Option;
import io.vavr.control.Try;

public interface FactRequest {
    Try<Option<Fact>> resolveEntity(EntityIdResolver resolver);
}
