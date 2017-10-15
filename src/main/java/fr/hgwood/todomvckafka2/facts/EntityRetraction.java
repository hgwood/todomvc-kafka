package fr.hgwood.todomvckafka2.facts;

import lombok.Value;

@Value
public class EntityRetraction implements Fact {
    private final String entity;
}
