package fr.hgwood.todomvckafka.actions;

import fr.hgwood.todomvckafka.actions.todoitem.AddTodo;
import fr.hgwood.todomvckafka.actions.todoitem.DeleteTodo;
import fr.hgwood.todomvckafka.facts.*;
import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;

import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_COMPLETED;
import static fr.hgwood.todomvckafka.schema.Attribute.TODO_ITEM_TEXT;

public class DeriveFacts implements ActionVisitor<Set<FactRequest>> {
    @Override
    public Set<FactRequest> visit(AddTodo action) {
        TemporaryEntityId temporaryEntityId = new TemporaryEntityId("new-todo");
        return HashSet.of(new AssertionRequest<>(temporaryEntityId,
                TODO_ITEM_TEXT,
                action.getText()
            ),
            new AssertionRequest<>(temporaryEntityId, TODO_ITEM_COMPLETED, false)
        );
    }

    @Override
    public Set<FactRequest> visit(DeleteTodo action) {
        return HashSet.of(new EntityRetractionRequest(new EntityLookup(Attribute.TODO_ITEM_ID,
            action.getId()
        )));
    }
}
