package fr.hgwood.todomvckafka.actions.todoitem;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.facts.*;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.Value;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;

@Value
public class AddTodo implements Action {
    private final String text;

    @Override
    public Set<FactRequest> deriveFacts() {
        TemporaryEntityId temporaryEntityId = new TemporaryEntityId("new-todo");
        return HashSet.of(
            new AssertionRequest<>(temporaryEntityId, TODO_ITEM_TEXT, this.text),
            new AssertionRequest<>(temporaryEntityId, TODO_ITEM_COMPLETED, false)
        );
    }
}
