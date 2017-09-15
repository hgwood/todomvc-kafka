package fr.hgwood.todomvckafka;

import lombok.Value;

@Value
public class Action {
    private final ActionType type;
    private final String id;
    private final String text;
}
