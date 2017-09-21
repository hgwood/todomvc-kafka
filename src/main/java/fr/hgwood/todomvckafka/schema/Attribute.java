package fr.hgwood.todomvckafka.schema;

import lombok.Value;

/**
 * This class makes up the schema of the entities stored by the system. It is inspired by Datomic.
 * Attribute associate a name (which should be unique) to a type. Attributes can be attached to any
 * entity through a {@link fr.hgwood.todomvckafka.facts.Fact}.
 *
 *
 * This should be an enum but enums don't support type parameters. So it falls back to constant
 * instances as static fields.
 *
 * @param <T> the type of the attribute's value.
 */
@Value
public class Attribute<T> {
    public static Attribute<String> TODO_ITEM_TEXT =
        new Attribute<>("todo-item", "text", String.class);
    public static Attribute<Boolean> TODO_ITEM_COMPLETED =
        new Attribute<>("todo-item", "completed", Boolean.class);

    private final String namespace;
    private final String name;
    private final Class<T> type;
}
