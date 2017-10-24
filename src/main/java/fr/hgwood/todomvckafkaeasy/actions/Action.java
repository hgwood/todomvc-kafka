package fr.hgwood.todomvckafkaeasy.actions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import fr.hgwood.todomvckafkaeasy.State;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "ADD_TODO", value = AddTodo.class),
    @JsonSubTypes.Type(name = "EDIT_TODO", value = EditTodo.class),
    @JsonSubTypes.Type(name = "DELETE_TODO", value = DeleteTodo.class)
})
public interface Action {
    State accept(ActionReducer reducer, State state);
}
