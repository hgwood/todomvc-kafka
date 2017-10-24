package fr.hgwood.todomvckafkaeasy.actions;

import fr.hgwood.todomvckafkaeasy.State;
import lombok.Value;

import java.util.UUID;

@Value
public class EditTodo implements Action {
    private final UUID id;
    private final String text;

    @Override
    public State accept(ActionReducer processor, State state) {
        return processor.apply(this, state);
    }
}
