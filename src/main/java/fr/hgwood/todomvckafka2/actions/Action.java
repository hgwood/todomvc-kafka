package fr.hgwood.todomvckafka2.actions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "ADD_TODO", value = AddTodo.class),
    @JsonSubTypes.Type(name = "DELETE_TODO", value = DeleteTodo.class)
})
public interface Action {
    <K> void accept(K key, ActionProcessor<K> processor);
}
