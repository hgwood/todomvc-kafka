package fr.hgwood.todomvckafka.actions.todoitem;

import fr.hgwood.todomvckafka.actions.Action;
import fr.hgwood.todomvckafka.facts.EntityRetraction;
import fr.hgwood.todomvckafka.facts.Fact;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.function.Supplier;

@Value
public class DeleteTodo implements Action {
    private final String id;

    @Override
    public Set<Fact> deriveFacts(Supplier<String> entityIdSupplier) {
        return HashSet.of(new EntityRetraction(this.id));
    }
}
