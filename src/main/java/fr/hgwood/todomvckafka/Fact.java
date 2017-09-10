package fr.hgwood.todomvckafka;

import fr.hgwood.todomvckafka.schema.Attribute;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

import static java.lang.String.format;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Fact {
    private final String entity;
    private final Attribute attribute;
    private final Object value;

    public static Fact of(String entity, Attribute attribute, Object value) {
        if (!attribute.isValidValue(value)) {
            throw new IllegalArgumentException(format(
                "value '%s' is not compatible with the type of attribute '%s'",
                value,
                attribute.getName()
            ));
        }
        return new Fact(entity, attribute, value);
    }
}
