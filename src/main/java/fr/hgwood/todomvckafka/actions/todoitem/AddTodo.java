package fr.hgwood.todomvckafka.actions.todoitem;

import fr.hgwood.todomvckafka.Fact;
import fr.hgwood.todomvckafka.actions.Action;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.function.Supplier;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;

@Value
@EqualsAndHashCode(callSuper = false)
public class AddTodo implements Action {
    private final String text;

    @Override
    public Set<Fact> deriveFacts(Supplier<String> entityIdSupplier) {
        String entity = entityIdSupplier.get();
        return HashSet.of(
            Fact.of(entity, TODO_ITEM_TEXT, this.text),
            Fact.of(entity, TODO_ITEM_COMPLETED, false)
        );
    }
}
