package fr.hgwood.todomvckafka;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Fact {
    private final String entity;
    private final String attribute;
    private final Object value;
}
