package fr.hgwood.todomvckafka2.actions;

import lombok.Value;

@Value
public class EditTodo implements Action {
    private final String id;
    private final String text;

    @Override
    public <K> void accept(K key, ActionProcessor<K> processor) {
        processor.process(key, this);
    }
}
