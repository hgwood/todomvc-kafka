package fr.hgwood.todomvckafka.actions.todoitem;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.facts.EntityLookup;
import fr.hgwood.todomvckafka.facts.EntityRetractionRequest;
import fr.hgwood.todomvckafka.facts.FactRequest;
import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.Value;

import java.util.UUID;

@Value
public class DeleteTodo implements Action {
    private final UUID id;

    @Override
    public Set<FactRequest> deriveFacts() {
        return HashSet.of(new EntityRetractionRequest(new EntityLookup(
            Attribute.TODO_ITEM_ID,
            this.id
        )));
    }
}
