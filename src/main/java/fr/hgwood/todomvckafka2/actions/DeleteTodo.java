package fr.hgwood.todomvckafka2.actions;

import lombok.Value;

@Value
public class DeleteTodo implements Action {
    private final String id;

    @Override
    public <K> void accept(K key, ActionProcessor<K> processor) {
        processor.process(key, this);
    }
}
