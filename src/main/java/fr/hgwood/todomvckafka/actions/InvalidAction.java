package fr.hgwood.todomvckafka.actions;

import lombok.Value;

@Value
public class InvalidAction {
    private final Action action;
    private final Iterable<Throwable> errors;
}
