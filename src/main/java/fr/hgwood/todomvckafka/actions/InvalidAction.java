package fr.hgwood.todomvckafka.actions;

import fr.hgwood.todomvckafka.facts.NoSuchEntity;
import lombok.Value;

@Value
public class InvalidAction {
    private final Action action;
    private final NoSuchEntity error;
}
