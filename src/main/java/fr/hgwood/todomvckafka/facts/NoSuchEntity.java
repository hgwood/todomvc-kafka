package fr.hgwood.todomvckafka.facts;

import lombok.Value;

@Value
public class NoSuchEntity {
    private final String entityId;
}
