package fr.hgwood.todomvckafka.actions.todoitem;

import fr.hgwood.todomvckafka.Fact;
import fr.hgwood.todomvckafka.actions.Action;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.function.Supplier;

@Value
@EqualsAndHashCode(callSuper = false)
public class DeleteTodo implements Action {
    private final String id;

    @Override
    public Set<Fact> deriveFacts(Supplier<String> entityIdSupplier) {
        return HashSet.of(Fact.retractEntity(this.id));
    }
}
