package fr.hgwood.todomvckafka2.facts;

import fr.hgwood.todomvckafka2.schema.Attribute;
import lombok.Value;

@Value
public class Assertion<T> implements Fact {
    private final String entity;
    private final Attribute<T> attribute;
    private final T value;
}
