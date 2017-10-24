package fr.hgwood.todomvckafkaeasy.actions;


import fr.hgwood.todomvckafkaeasy.State;

public interface ActionReducer {
    State apply(AddTodo action, State state);

    State apply(EditTodo action, State state);

    State apply(DeleteTodo action, State state);

    State apply(CompleteAll action, State state);

    State apply(ClearCompleted action, State state);
}
