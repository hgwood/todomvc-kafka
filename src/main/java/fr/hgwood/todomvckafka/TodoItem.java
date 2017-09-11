package fr.hgwood.todomvckafka;

import lombok.Value;

@Value
public class TodoItem {
    private final String text;
    private final boolean completed;
}
