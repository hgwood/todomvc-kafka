package fr.hgwood.todomvckafka2.schema;

import lombok.Value;

@Value
public class Attribute<T> {
    public static Attribute<String> TODO_ITEM_ID =
        new Attribute<>("todo-item", "id", String.class);
    public static Attribute<String> TODO_ITEM_TEXT =
        new Attribute<>("todo-item", "text", String.class);
    public static Attribute<Boolean> TODO_ITEM_COMPLETED =
        new Attribute<>("todo-item", "completed", Boolean.class);

    private final String namespace;
    private final String name;
    private final Class<T> type;
}