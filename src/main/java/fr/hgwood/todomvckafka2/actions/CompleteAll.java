package fr.hgwood.todomvckafka2.actions;

import lombok.Value;

@Value
public class CompleteAll implements Action {
    @Override
    public <K> void accept(K key, ActionProcessor<K> processor) {
        processor.process(key, this);
    }
}
