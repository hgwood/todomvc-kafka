package fr.hgwood.todomvckafka.actions.todoitem;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.facts.ValueAssertion;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.function.Supplier;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;

@Value
public class AddTodo implements Action {
    private final String text;

    @Override
    public Set<Fact> deriveFacts(Supplier<String> entityIdSupplier) {
        String entity = entityIdSupplier.get();
        return HashSet.of(
            new ValueAssertion<>(entity, TODO_ITEM_TEXT, this.text),
            new ValueAssertion<>(entity, TODO_ITEM_COMPLETED, false)
        );
    }
}
