package fr.hgwood.todomvckafka.actions.todoitem;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.facts.EntityLookup;
import fr.hgwood.todomvckafka.facts.EntityRetraction;
import fr.hgwood.todomvckafka.facts.Fact;
import fr.hgwood.todomvckafka.schema.Attribute;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.UUID;
import java.util.function.Supplier;

@Value
public class DeleteTodo implements Action {
    private final UUID id;

    @Override
    public Set<Fact> deriveFacts() {
        return HashSet.of(new EntityRetraction(new EntityLookup(Attribute.TODO_ITEM_ID, this.id)));
    }
}
