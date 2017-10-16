package fr.hgwood.todomvckafka2.actions;

public class ClearCompleted implements Action {

    @Override
    public <K> void accept(K key, ActionProcessor<K> processor) {
        processor.process(key, this);
    }
}
