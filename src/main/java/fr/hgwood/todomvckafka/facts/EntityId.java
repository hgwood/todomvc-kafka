package fr.hgwood.todomvckafka.facts;

import lombok.Value;

import java.util.UUID;

@Value
public class EntityId {
    private final String value;
}
