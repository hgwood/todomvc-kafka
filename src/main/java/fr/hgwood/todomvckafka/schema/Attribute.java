package fr.hgwood.todomvckafka.schema;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Attribute {
    TODO_ITEM_TEXT("todo-item/text", String.class),
    TODO_ITEM_COMPLETED("todo-item/completed", Boolean.class);

    private final String name;
    private final Class<?> type;

    public boolean isValidValue(Object value) {
        return this.type.isAssignableFrom(value.getClass());
    }

    @JsonValue
    public String getName() {
        return this.name;
    }
}
