package fr.hgwood.todomvckafka.actions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import fr.hgwood.todomvckafka.actions.todoitem.AddTodo;
import fr.hgwood.todomvckafka.actions.todoitem.DeleteTodo;
import fr.hgwood.todomvckafka.facts.Fact;
import io.vavr.collection.Set;

import java.util.function.Supplier;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "ADD_TODO", value = AddTodo.class),
    @JsonSubTypes.Type(name = "DELETE_TODO", value = DeleteTodo.class)
})
public interface Action {
    Set<Fact> deriveFacts(Supplier<String> entityIdSupplier);
}
