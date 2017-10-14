package fr.hgwood.todomvckafka.facts;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import fr.hgwood.todomvckafka.actions.ActionVisitor;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(name = "assertion", value = AssertionRequest.class),
    @JsonSubTypes.Type(name = "accretion", value = AccretionRequest.class),
    @JsonSubTypes.Type(name = "entity-retraction", value = EntityRetractionRequest.class)
})
public interface FactRequest {
    Either<NoSuchEntity, Fact> resolveEntity(EntityIdResolver resolver);
    <R> R accept(FactRequestVisitor<R> visitor);
}
