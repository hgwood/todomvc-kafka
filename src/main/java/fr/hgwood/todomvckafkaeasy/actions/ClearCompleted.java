package fr.hgwood.todomvckafkaeasy.actions;

import fr.hgwood.todomvckafkaeasy.State;

public class ClearCompleted implements Action {

    @Override
    public State accept(ActionReducer processor, State state) {
        return processor.apply(this, state);
    }
}
