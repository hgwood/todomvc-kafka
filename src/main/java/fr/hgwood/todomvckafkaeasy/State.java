package fr.hgwood.todomvckafkaeasy;

import io.vavr.collection.List;
import lombok.Value;

import java.util.UUID;

@Value
public class State {
    private final List<TodoItem> todoItems;

    @Value
    public static class TodoItem {
        private final String text;
        private final boolean completed;
        private final UUID id;
    }
}
