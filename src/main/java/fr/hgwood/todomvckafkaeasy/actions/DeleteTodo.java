package fr.hgwood.todomvckafkaeasy.actions;

import fr.hgwood.todomvckafkaeasy.State;
import lombok.Value;

import java.util.UUID;

@Value
public class DeleteTodo implements Action {
    private final UUID id;

    @Override
    public State accept(ActionReducer processor, State state) {
        return processor.apply(this, state);
    }
}
