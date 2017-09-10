package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.control.Option;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

import static fr.hgwood.todomvckafka.Fact.FactKind.ASSERTION;
import static fr.hgwood.todomvckafka.Fact.FactKind.RETRACTION;
import static java.lang.String.format;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Fact {
    private final FactKind kind;
    private final String entity;
    private final Option<Attribute> attribute;
    private final Option<Object> value;

    public static Fact of(String entity, Attribute attribute, Object value) {
        requireValidValue(attribute, value);
        return new Fact(
            ASSERTION,
            entity,
            Option.of(attribute),
            Option.of(value)
        );
    }

    public static Fact retractEntity(String entity) {
        return new Fact(RETRACTION, entity, Option.none(), Option.none());
    }

    private static void requireValidValue(Attribute attribute, Object value) {
        if (!attribute.isValidValue(value)) {
            throw new IllegalArgumentException(format(
                "value '%s' is not compatible with the type of attribute '%s'",
                value,
                attribute.getName()
            ));
        }
    }

    public enum FactKind {
        ASSERTION,
        RETRACTION
    }
}
