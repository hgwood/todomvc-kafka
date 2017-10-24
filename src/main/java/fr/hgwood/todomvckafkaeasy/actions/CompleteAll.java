package fr.hgwood.todomvckafkaeasy.actions;

import fr.hgwood.todomvckafkaeasy.State;
import lombok.Value;

@Value
public class CompleteAll implements Action {
    @Override
    public State accept(ActionReducer processor, State state) {
        return processor.apply(this, state);
    }
}
