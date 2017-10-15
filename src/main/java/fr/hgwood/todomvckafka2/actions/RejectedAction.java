package fr.hgwood.todomvckafka2.actions;

import lombok.Value;

@Value
public class RejectedAction {
    private final Action action;
    private final String reason;
}
