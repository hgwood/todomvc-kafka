package fr.hgwood.todomvckafkaeasy.actions;

import fr.hgwood.todomvckafkaeasy.State;
import lombok.Value;

@Value
public class AddTodo implements Action {
    private final String text;

    @Override
    public State accept(ActionReducer processor, State state) {
        return processor.apply(this, state);
    }
}
